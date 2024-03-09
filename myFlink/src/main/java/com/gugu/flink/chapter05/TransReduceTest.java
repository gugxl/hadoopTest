package com.gugu.flink.chapter05;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransReduceTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.addSource(new MySourceFunction())
            .map(new MapFunction<Event, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(Event event) throws Exception {
                    return Tuple2.of(event.getUser(), 1L);
                }
            })
            .keyBy(r -> r.f0)   // 使用用户名来进行分流
            .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                    // 每到一条数据，用户 pv 的统计值加 1
                    return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                }
            })
            .keyBy(r -> true) // 为每一条数据分配同一个 key，将聚合结果发送到一条流中
            .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                    // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                    return t1.f1 > t2.f1 ? t1 : t2;
                }
            })
            .print();

        environment.execute();
    }
}
