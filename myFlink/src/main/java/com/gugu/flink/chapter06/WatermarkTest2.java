package com.gugu.flink.chapter06;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkTest2 {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        environment.socketTextStream("192.168.2.100", 7777)
            .map(new MapFunction<String, Event>() {
                @Override
                public Event map(String value) throws Exception {
                    String[] split = value.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                }
            })
            .assignTimestampsAndWatermarks(
                // 针对乱序流插入水位线，延迟时间设置为 5s
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.getTimestamp();
                        }
                    })
            )
            .keyBy(data -> data.getUser())
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new WatermarkTestResult())
            .print();

        environment.execute();
    }

    static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long currentWatermark = context.currentWatermark();
            long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，窗口闭合计算时，水位线处于："
                + currentWatermark);
        }
    }
}
