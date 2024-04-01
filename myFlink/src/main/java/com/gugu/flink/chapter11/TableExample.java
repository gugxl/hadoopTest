package com.gugu.flink.chapter11;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = environment.fromElements(new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=100", 3000L),
            new Event("Bob", "./prod?id=2", 4000L),
            new Event("Cary", "./prod?id=1", 5000L),
            new Event("Bob", "./prod?id=3", 6000L)
        );
        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);
        // 将数据流转换成表
        //Table table = tableEnv.fromDataStream(eventDataStreamSource);
        //// 用执行 SQL 的方式提取数据
        //Table visitTable = tableEnv.sqlQuery("select url, user from " + table);
        //tableEnv.toRetractStream(visitTable, Event.class).print();

        environment.execute();
    }
}
