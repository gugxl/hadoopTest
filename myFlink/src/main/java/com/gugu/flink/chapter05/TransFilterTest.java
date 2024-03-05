package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFilterTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();

        DataStreamSource<Event> streamSource = environment.fromElements(
            new Event("gugu", "./home", 100L),
            new Event("xiaoke", "/page", 150L));
        streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return "gugu".equals(event.getUser());
            }
        }).print();

        streamSource.filter(new MyFilter()).print();

        environment.execute();
    }
static class MyFilter implements FilterFunction<Event>{

    @Override
    public boolean filter(Event event) throws Exception {
        return "xiaoke".equals(event.getUser());
    }
}
}
