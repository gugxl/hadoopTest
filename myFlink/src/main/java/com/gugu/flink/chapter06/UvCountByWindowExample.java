package com.gugu.flink.chapter06;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class UvCountByWindowExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = environment.addSource(new MySourceFunction())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
        // 将数据全部发往同一分区，按窗口统计UV
        streamOperator
            .keyBy(data -> true)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new UvCountByWindow())
                .print();

        environment.execute();
    }

    static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Set<String> userSet = new HashSet<>();

            for (Event element : elements) {
                userSet.add(element.getUser());
            }
            // 结合窗口信息，包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("窗口" + new Timestamp(start) + "~" + new Timestamp(end) + "的独立访客数量：" + userSet.size());
        }
    }
}
