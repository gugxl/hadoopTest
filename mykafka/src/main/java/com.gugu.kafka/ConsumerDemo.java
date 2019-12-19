package com.gugu.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author gugu
 * @Classname ConsumerDemo
 * @Description TODO
 * @Date 2019/12/19 17:04
 */
public class ConsumerDemo {
    private static final String topic = "my-topic";
    private static final Integer threads = 2;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect","master:2181,slave1:2181,slave2:2181");
        properties.put("group.id","gugu");
        //smallest重最开始消费,largest代表重消费者启动后产生的数据才消费
        //--from-beginning
        properties.put("auto.offset.reset","smallest");
        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic,threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);
        for (KafkaStream<byte[], byte[]> kafkaStream:kafkaStreams){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> mm:kafkaStream){
                        String msg = new String(mm.message());
                        System.out.println(msg);
                    }
                }
            }).start();
        }
    }
}
