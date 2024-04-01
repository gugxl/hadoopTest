package com.gugu.flink.chapter11;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WindowTopNExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = environment.fromElements(
            new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=100", 3000L),
            new Event("Bob", "./prod?id=1", 3300L),
            new Event("Alice", "./prod?id=200", 3200L),
            new Event("Bob", "./home", 3500L),
            new Event("Bob", "./prod?id=2", 3800L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
            .<Event>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.getTimestamp();
                }
            }));
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);
        // 将数据流转换成表，并指定时间属性
        Table table = tableEnv.fromDataStream(eventSingleOutputStreamOperator, "user,url,ts.rowtime as ts");
        // 为 方 便 在 SQL 中 引 用 ， 在 环 境 中 注 册 表 EventTable
        tableEnv.createTemporaryView("EventTable", table);
        // 定义子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
        String subQuery =
            "SELECT window_start, window_end, user, COUNT(url) as cnt " + "FROM TABLE ( " +
                " TUMBLE( TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR )) " +
                "GROUP BY window_start, window_end, user ";
        // 定义 Top N 的外层查询
        String topNQuery = "SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY window_start, window_end "
            + "ORDER BY cnt desc ) AS row_num FROM (" + subQuery + ")) " + "WHERE row_num <= 2";
        // 执行 SQL 得到结果表
        Table result = tableEnv.sqlQuery(topNQuery);
        tableEnv.toDataStream(result).print();
        environment.execute();
    }
}
