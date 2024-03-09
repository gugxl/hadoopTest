package com.gugu.flink.chapter05;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        // 读取数据源，并行度为 1
        DataStreamSource<Event> eventDataStreamSource = environment.addSource(new MySourceFunction());
        // 经 轮 询 重 分 区 后 打 印 输 出 ， 并 行 度 为 4
        eventDataStreamSource.rebalance().print("rebalance").setParallelism(4);

        environment.execute();
    }
}
