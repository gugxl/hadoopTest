package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmapTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();

        DataStreamSource<Event> streamSource = environment.fromElements(
            new Event("gugu", "./home", 100L),
            new Event("xiaoke", "/page", 150L));

        streamSource.flatMap(new MyFlatMap()).print();
        environment.execute();
    }

    static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            if (event.getUser().equals("gugu")){
                collector.collect(event.getUser());
            } else if ("xiaoke".equals(event.getUser())){
                collector.collect(event.getUser());
                collector.collect(event.getUrl());
            }
        }
    }
}
