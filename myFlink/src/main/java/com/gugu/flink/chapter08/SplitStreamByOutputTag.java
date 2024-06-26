package com.gugu.flink.chapter08;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStreamByOutputTag {
    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<>("Mary-pv") {
    };
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<>("Mary-pv") {
    };

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> stream = environment.addSource(new MySourceFunction());
        SingleOutputStreamOperator<Event> process = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.getUser().equals("Mary")) {
                    ctx.output(MaryTag, new Tuple3<>(value.getUser(), value.getUrl(), value.getTimestamp()));
                } else if (value.getUser().equals("Bob")) {
                    ctx.output(BobTag, new Tuple3<>(value.getUser(), value.getUrl(), value.getTimestamp()));
                } else {
                    out.collect(value);
                }
            }
        });

        process.getSideOutput(MaryTag).print("Mary pv");
        process.getSideOutput(BobTag).print("Bob pv");
        process.print("else");
        environment.execute();
    }
}
