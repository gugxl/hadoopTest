package com.gugu.flink.chapter07;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class EventTimeTimerTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = environment
            .addSource(new CustomSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
        // 基于 KeyedStream 定义事件时间定时器
        stream
            .keyBy(data -> true)
            .process(new KeyedProcessFunction<Boolean, Event, String>() {
                @Override
                public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                    out.collect("数据到达，时间戳为：" + ctx.timestamp());
                    out.collect("数据到达，水位线为：" + ctx.timerService().currentWatermark());
                    // 注册一个 10 秒后的定时器
                    ctx.timerService().registerProcessingTimeTimer(ctx.timestamp());
                }

                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                    out.collect("定时器触发，触发时间：" + timestamp);
                }
            })
            .print();
        environment.execute();
    }
}
