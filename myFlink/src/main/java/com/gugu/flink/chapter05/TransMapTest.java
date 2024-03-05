package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> streamSource = environment.fromElements(new Event("gugu", "./home", 100L), new Event("xiaoke", "/page", 150L));
        //传入匿名类，实现 MapFunction
        streamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.getUser();
            }
        });

        //传入 MapFunction 的实现类
        streamSource.map(new UserFunction()).print();

        environment.execute();
    }
}
