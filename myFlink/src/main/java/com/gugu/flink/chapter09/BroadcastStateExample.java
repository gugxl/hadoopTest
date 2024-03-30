package com.gugu.flink.chapter09;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Action> actionStream = environment.fromElements(
            new Action("login", "Alice"),
            new Action("pay", "Alice"),
            new Action("login", "Bob"),
            new Action("buy", "Bob"),
            new Action("pay", "Bob")
        );

        // 定义行为模式流，代表了要检测的标准
        DataStreamSource<Pattern> patternStream = environment.fromElements(new Pattern("login", "pay"),
            new Pattern("login", "buy")
        );

        // 定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));

        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);
        // 将事件流和广播流连接起来，进行处理
        SingleOutputStreamOperator<Tuple2<String, Pattern>> process = actionStream.keyBy(data -> data.getUserId())
            .connect(bcPatterns)
            .process(new PatternEvaluator());


        environment.execute();
    }

    static class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        // 定义一个值状态，保存上一次用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction", Types.STRING));
        }

        @Override
        public void processElement(Action action, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

            MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));

            Pattern pattern = ctx.getBroadcastState(bcStateDescriptor).get(null);
            String prevAction = prevActionState.value();
            // 如果前后两次行为都符合模式定义，输出一组匹配
            if (prevAction != null && pattern != null && prevAction.equals(pattern.getAction1()) && action.getAction().equals(pattern.getAction2())) {
                out.collect(new Tuple2<>(action.getUserId(), pattern));
            }
            prevActionState.update(action.action);
        }

        @Override
        public void processBroadcastElement(Pattern pattern, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
            BroadcastState<Void, Pattern> bcState = ctx
                .getBroadcastState(bcStateDescriptor);

            // 将广播状态更新为当前的 pattern
            bcState.put(null, pattern);
        }
    }

    @Data
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    static class Action {
        private String action;
        private String userId;
    }

    @Data
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    static class Pattern {
        private String action1;
        private String action2;
    }
}
