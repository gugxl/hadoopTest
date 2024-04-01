package com.gugu.flink.chapter11;

import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CumulateWindowExample {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
            new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=100", 3000L),
            new Event("Bob", "./prod?id=1", 3300L),
            new Event("Alice", "./prod?id=200", 3200L),
            new Event("Bob", "./prod?id=2", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
            .<Event>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.getTimestamp();
                }
            })
        );
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表，并指定时间属性
        tableEnv.createTemporaryView("event_table", eventStream, "user, url, timestamp.rowtime");

        Table result = tableEnv.sqlQuery("SELECT " +
            "user, " +
            "window_end AS endT, " + "COUNT(url) AS cnt " +
            "FROM TABLE( " +
            "CUMULATE( TABLE EventTable, " +    // 定义累积窗口
            "DESCRIPTOR(ts), " + "INTERVAL '30' MINUTE, " + "INTERVAL '1' HOUR)) " +
            "GROUP BY user, window_start, window_end "
        );

        tableEnv.toDataStream(result, String.class).print();

        env.execute();
    }
}
