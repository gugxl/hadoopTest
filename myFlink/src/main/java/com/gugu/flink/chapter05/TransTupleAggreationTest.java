package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransTupleAggreationTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        DataStreamSource<Tuple2<String, Integer>> stream = environment.fromElements(
            Tuple2.of("a", 1),
            Tuple2.of("b", 2),
            Tuple2.of("a", 3),
            Tuple2.of("d", 4)
        );
        stream.keyBy(x -> x.f0).sum(1).print();
        stream.keyBy(x -> x.f0).sum("f1").print();
        stream.keyBy("f0").max("f1").print();
        stream.keyBy("f0").min(1).print();
        stream.keyBy("f0").maxBy(1).print();


        environment.execute();
    }
}
