package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFunctionUDFTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();

        DataStreamSource<Event> eventDataStreamSource = environment.fromElements(new Event("gugu", "/home", 100L),
            new Event("zhangshan", "/index", 500l));

        SingleOutputStreamOperator<Event> filter = eventDataStreamSource.filter(new FlinkFilter());
        filter.print();
        eventDataStreamSource.filter(new KeyWordFilter("home")).print();

        environment.execute();
    }

    static class FlinkFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return event.getUser().equals(
                "gugu"
            );
        }
    }

    static class KeyWordFilter implements FilterFunction<Event> {
        private String keyWord;

        public KeyWordFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.getUrl().contains(keyWord);
        }
    }
}
