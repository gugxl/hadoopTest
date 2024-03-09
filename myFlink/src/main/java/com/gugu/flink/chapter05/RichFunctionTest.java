package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(2);
        DataStreamSource<Event> clicks = environment.fromElements(
            new Event("gugu", "/home", 100L),
            new Event("Bob", "/home", 100L),
            new Event("Alice", "/home", 100L),
            new Event("Cary", "/home", 100L),
            new Event("xiaoke", "/home", 100L)
        );

        clicks.map(new RichMapFunction<Event, Long>() {

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + "任务开始");
                }

                @Override
                public Long map(Event event) throws Exception {
                    return event.getTimestamp();
                }

                @Override
                public void close() throws Exception {
                    super.close();
                    System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + "任务结束");
                }
            })
            .print();

        environment.execute();
    }
}
