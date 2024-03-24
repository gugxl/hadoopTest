package com.gugu.flink.chapter06;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowReduceExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        DataStreamSource<Event> eventDataStreamSource = environment.addSource(new MySourceFunction());

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = eventDataStreamSource
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {

                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

        eventSingleOutputStreamOperator.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.getUser(), 1L);
            }
        })
            .keyBy(r -> r.f0)
        // 设置滚动事件时间窗口
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t2, Tuple2<String, Long> t1) throws Exception {
                        // 定义累加规则，窗口闭合时，向下游发送累加结果
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                }).print();

            environment.execute();
    }
}
