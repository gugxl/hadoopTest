package com.gugu.flink.chapter11;

import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AppendQueryExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.fromElements(new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=100", 3000L),
            new Event("Bob", "./prod?id=1", 3300L),
            new Event("Alice", "./prod?id=200", 3500L),
            new Event("Bob", "./home", 3600L),
            new Event("Bob", "./prod?id=2", 3800L),
            new Event("Bob", "./prod?id=3", 4200L)
        ).assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        // 将数据流转换成表，并指定时间属性

        // 为 方 便 在 SQL 中 引 用 ， 在 环 境 中 注 册 表 EventTable
        streamTableEnvironment.createTemporaryView("EventTable", eventSingleOutputStreamOperator, "user, url, ts.rowtime as ts");
        // 设置 1 小时滚动窗口，执行 SQL 统计查询
        Table result = streamTableEnvironment.sqlQuery("SELECT " +
            "user, " +
            "window_end AS endT, " +    // 窗口结束时间
            "COUNT(url) AS cnt " +    // 统计 url 访问次数
            "FROM TABLE( " +
            "TUMBLE( TABLE EventTable, " +    // 1 小时滚动窗口
            "DESCRIPTOR(ts), " + "INTERVAL '1' HOUR)) " +
            "GROUP BY user, window_start, window_end "
        );
        streamTableEnvironment.toDataStream(result).print();

        env.execute();
    }
}
