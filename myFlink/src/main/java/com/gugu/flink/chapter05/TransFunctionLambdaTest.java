package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class TransFunctionLambdaTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        DataStreamSource<Event> eventDataStreamSource = environment.fromElements(new Event("gugu", "/home", 100L),
            new Event("xiaokeai", "/index", 10L));
        eventDataStreamSource.map(x -> x.getUser())
            .print();
        //// 异常 不知道 Collector 类型
        //eventDataStreamSource.flatMap((event, out) -> {
        //        out.collect(event.getUrl());
        //    })
        //    .print();

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = eventDataStreamSource.flatMap((Event event, Collector<String> out) -> {
                out.collect(event.getUrl());
            })
            .returns(Types.STRING);
        stringSingleOutputStreamOperator.print();

        //// 异常 不知道 Tuple2类型
        //SingleOutputStreamOperator<Tuple2<String, Long>> map = eventDataStreamSource.map(event -> {
        //    return Tuple2.of(event.getUser(), 1L);
        //});
        //map.print();

        // 1) 使 用 显 式 的 ".returns(...)"
        SingleOutputStreamOperator<Tuple2<String, Long>> returnsType = eventDataStreamSource
            .map(event -> Tuple2.of(event.getUser(), 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG));
        returnsType.print();
        // 2) 使用类来替代 Lambda 表达式
        eventDataStreamSource.map(new MyTuple2Mapper()).print();
        // 3) 使用匿名类来代替 Lambda 表达式
        eventDataStreamSource.map(new MapFunction<Event, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(Event event) throws Exception {
                    return Tuple2.of(event.getUser(), 1L);
                }
            })
            .print();


        environment.execute();

    }

    private static class MyTuple2Mapper implements MapFunction<Event, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(Event event) throws Exception {
            return Tuple2.of(event.getUser(), 1L);
        }
    }
}
