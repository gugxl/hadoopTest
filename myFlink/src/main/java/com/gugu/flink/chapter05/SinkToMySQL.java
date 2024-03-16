package com.gugu.flink.chapter05;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = environment.addSource(new MySourceFunction());
        eventDataStreamSource.addSink(JdbcSink.sink(" INSERT INTO clicks (user, url) VALUES (?, ?) ", new CustomJdbcStatementBuilder(), new JdbcConnectionOptions
            .JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:mysql://192.168.2.3:3306/mybatis_plus")
            .withDriverName("com.mysql.cj.jdbc.Driver")
            .withUsername("gugu")
            .withPassword("asd")
            .build()
        ));

        environment.execute();
    }
}

class CustomJdbcStatementBuilder implements JdbcStatementBuilder<Event> {
    @Override
    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
        preparedStatement.setString(1, event.getUser());  // 假设 user 对应的字段索引为 1
        preparedStatement.setString(2, event.getUrl());   // 假设 url 对应的字段索引为 2
    }
}