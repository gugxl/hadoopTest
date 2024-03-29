package com.gugu.flink.chapter08;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CoGroupExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = environment.fromElements(
            Tuple2.of("a", 1000L),
            Tuple2.of("b", 1000L),
            Tuple2.of("a", 2000L),
            Tuple2.of("b", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                    return element.f1;
                }
            }));

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = environment.fromElements(
            Tuple2.of("a", 3000L),
            Tuple2.of("b", 3000L),
            Tuple2.of("a", 4000L),
            Tuple2.of("b", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                    return element.f1;
                }
            })
        );

        stream1
            .coGroup(stream2)
            .where(r -> r.f0)
            .equalTo(r -> r.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                @Override
                public void coGroup(Iterable<Tuple2<String, Long>> first, Iterable<Tuple2<String, Long>> second, Collector<String> out) throws Exception {
                    out.collect(first + " => " + second);
                }
            })
            .print();

        environment.execute();
    }
}
