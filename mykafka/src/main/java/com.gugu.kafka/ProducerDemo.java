package com.gugu.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author gugu
 * @Classname ProducerDemo
 * @Description TODO
 * @Date 2019/12/19 19:11
 */
public class ProducerDemo {
    private static final String topic = "my-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list","master:9092,slave1:9092,slave2:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<>(producerConfig);
        for (int i = 0; i < 1000; i++) {
            producer.send(new KeyedMessage<String,String>(topic,"gugu"+i));
        }
    }
}
