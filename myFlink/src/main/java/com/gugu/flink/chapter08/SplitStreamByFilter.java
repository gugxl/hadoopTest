package com.gugu.flink.chapter08;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitStreamByFilter {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> stream = environment.addSource(new MySourceFunction());
        // 筛选 Mary 的浏览行为放入 MaryStream 流中
        SingleOutputStreamOperator<Event> maryStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getUser().equals("Mary");
            }
        });
        // 筛选 Bob 的购买行为放入 BobStream 流中
        SingleOutputStreamOperator<Event> bobStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getUser().equals("Bob");
            }
        });
        // 筛选其他人的浏览行为放入 elseStream 流中
        SingleOutputStreamOperator<Event> elseStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return !value.getUser().equals("Mary") && !value.getUser().equals("Bob");
            }
        });

        maryStream.print("Mary pv");
        bobStream.print("Bob pv");
        elseStream.print("else pv");
        environment.execute();
    }
}
