package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyByTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        DataStreamSource<Event> streamSource = environment.fromElements(
            new Event("gugu", "./home", 100L),
            new Event("xiaoke", "/page", 150L));
        //使用 Lambda 表达式
        KeyedStream<Event, String> eventStringKeyedStream = streamSource.keyBy(event -> event.getUser());
        // 使用匿名类
        streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.getUser();
            }
        }).print();


        environment.execute();
    }
}
