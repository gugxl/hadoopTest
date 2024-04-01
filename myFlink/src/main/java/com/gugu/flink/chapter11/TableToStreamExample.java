package com.gugu.flink.chapter11;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableToStreamExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> streamSource = environment.fromElements(new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=1", 5 * 1000L)
        );
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment);
        // 将数据流转换成表
        streamTableEnvironment.createTemporaryView("EventTable", streamSource);
        // 查询 Alice 的访问 url 列表
        Table aliceVisitTable = streamTableEnvironment.sqlQuery("select url from EventTable where user = 'Alice'");
        // 统计每个用户的点击次数
        Table urlCountTable = streamTableEnvironment.sqlQuery("SELECT user, COUNT(url) FROM EventTable GROUP BY user");
        // 将表转换成数据流，在控制台打印输出
        streamTableEnvironment.toDataStream(aliceVisitTable).print("alice visit");
        streamTableEnvironment.toDataStream(urlCountTable).print("count");

        environment.execute();
    }
}
