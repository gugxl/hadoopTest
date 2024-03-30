package com.gugu.flink.chapter09;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TwoStreamFullJoinExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = environment.fromElements(
            Tuple3.of("a", "stream-1", 1000L),
            Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                @Override
                public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                    return element.f2;
                }
            })
        );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = environment.fromElements(
            Tuple3.of("a", "stream-2", 3000L),
            Tuple3.of("b", "stream-2", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                @Override
                public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                    return element.f2;
                }
            })
        );

        stream1.keyBy(r -> r.f0)
            .connect(stream2.keyBy(r -> r.f0))
            .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                private ListState<Tuple3<String, String, Long>> stream1ListState;
                private ListState<Tuple3<String, String, Long>> stream2ListState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING)));
                    stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING)));
                }

                @Override
                public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                    stream1ListState.add(value);
                    for (Tuple3<String, String, Long> stringStringLongTuple3 : stream2ListState.get()) {
                        out.collect(value + " => " + stringStringLongTuple3);
                    }
                }

                @Override
                public void processElement2(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                    stream2ListState.add(value);
                    for (Tuple3<String, String, Long> stringStringLongTuple3 : stream1ListState.get()) {
                        out.collect(stringStringLongTuple3 + " => " + value);
                    }
                }
            })
            .print();

        environment.execute();
    }
}
