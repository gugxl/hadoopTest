package com.gugu.flink.chapter12;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class NFAExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        KeyedStream<LoginEvent, String> stream = environment.fromElements(new LoginEvent("user_1", "192.168.0.1", "fail", 1000L),
            new LoginEvent("user_1", "192.168.0.2", "fail", 2000L),
            new LoginEvent("user_2", "192.168.0.3", "fail", 3000L),
            new LoginEvent("user_1", "192.168.0.4", "fail", 4000L)
        ).keyBy(event -> event.getUserId());
        // 将数据依次输入状态机进行处理
        SingleOutputStreamOperator<String> alertStream  = stream.flatMap(new StateMachineMapper());
        alertStream.print("alertStream warning");

        environment.execute();

    }

    static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {
        // 声明当前用户对应的状态
        private ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取状态对象
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent value, Collector<String> out) throws Exception {
            // 获取状态，如果状态为空，置为初始状态
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }
            // 基于当前状态，输入当前事件时跳转到下一状态
            State nextState = state.transition(value.getEventType());

            if (state == State.Matched) {
                // 如果检测到匹配的复杂事件，输出报警信息
                out.collect("用户 " + value.getUserId() + " 连续三次登录失败，请检查账号是否被盗");
            } else if (state == State.Terminal) {
                // 如果到了终止状态，就重置状态，准备重新开始
                state = State.Initial;
            } else {
                // 如果还没结束，更新状态（状态跳转），继续读取事件
                currentState.update(nextState);

            }
        }
    }

    static enum State {
        Terminal,    // 匹配失败，当前匹配终止

        Matched,    // 匹配成功

        // S2 状态
        S2(new Transition("fail", Matched), new Transition("success", Terminal)),

        // S1 状态
        S1(new Transition("fail", S2), new Transition("success", Terminal)),

        // 初始状态
        Initial(new Transition("fail", S1), new Transition("success", Terminal));

        private final Transition[] transitions;    // 状态转移规则

        // 状态的构造方法，可以传入一组状态转移规则来定义状态
        State(Transition... transitions) {
            this.transitions = transitions;
        }

        // 状态的转移方法，根据当前输入事件类型，从定义好的转移规则中找到下一个状态
        public State transition(String eventType) {
            for (Transition t : transitions) {
                if (t.getEventType().equals(eventType)) {
                    return t.getTargetState();
                }
            }
            // 如果没有找到转移规则，说明已经结束，回到初始状态
            return Initial;
        }


    }

    @Getter
    // 定义状态转移类，包括两个属性：当前事件类型和目标状态
    static class Transition {
        private final String eventType;    // 状态转移的输入事件类型
        private final State targetState;    // 状态转移的目标状态

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }

    }
}
