package com.gugu.flink;

import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceKafkaTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
//        kafka-console-producer.sh --broker-list  master:9092 --topic my-topic
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "master.com:9092");
        properties.setProperty("group.id", "consumer-gugu");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");
        DataStreamSource<String> stream = environment.addSource(new FlinkKafkaConsumer<String>("my-topic", new SimpleStringSchema(), properties));
        stream.print();
        environment.execute();
    }
}
