package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

public class SinkCustomtoHBase {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        environment
            .fromElements("hello", "world")
            .addSink(new RichSinkFunction<String>() {
                private org.apache.hadoop.conf.Configuration configuration;// 管理 Hbase 的配置信息
                public Connection connection;// 管理 Hbase 连接

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    configuration = HBaseConfiguration.create();
                    configuration.set("hbase.zookeeper.quorum", "192.168.2.100");
                    connection  = ConnectionFactory.createConnection(configuration);
                }

                @Override
                public void invoke(String value, Context context) throws Exception {
                    Table table = connection.getTable(TableName.valueOf("t_student"));
                    Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8));
                    put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                        "content".getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8));
                    table.put(put);
                    table.close();
                }

                @Override
                public void close() throws Exception {
                    super.close();
                    connection.close();
                }
            });

        environment.execute();
    }
}
