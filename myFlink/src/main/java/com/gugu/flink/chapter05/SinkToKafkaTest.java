package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafkaTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.2.100:9092");
        properties.setProperty("bootstrap.servers", "192.168.2.100:9092");
        DataStreamSource<String> eventDataStreamSource = environment.readTextFile("D:\\applicationfiles\\data\\people.txt");
        eventDataStreamSource
            .addSink(new FlinkKafkaProducer<String>("clicks", new SimpleStringSchema(), properties));


        environment.execute();
    }
}
