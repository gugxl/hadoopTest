package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomPartitionTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
            .partitionCustom(new Partitioner<Integer>() {
                @Override
                public int partition(Integer key, int numPartations) {
                    return key % 2;
                }
            }, new KeySelector<Integer, Integer>() {
                @Override
                public Integer getKey(Integer value) throws Exception {
                    return value;
                }
            })
            .print()
            .setParallelism(2);
        environment.execute();
    }
}
