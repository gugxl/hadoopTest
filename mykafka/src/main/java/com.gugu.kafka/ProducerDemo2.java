package com.gugu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

;

public class ProducerDemo2 {
    static Properties properties = new Properties();
    static {
        properties.put("bootstrap.servers","master:9092,slave1:9092,slave2:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.gugu.kafka.CustomerSerializer");
    }
    static KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    public static void main(String[] args)  {
//        sendMsg();
        sendMsgByAsync();
//        sendMsgBySync();
    }


    public static void sendMsg()  {
        ProducerRecord producerRecord = new ProducerRecord("my-topic", "test message");
        producer.send(producerRecord);
        System.out.println("message send");
    }

    /**
     * 同步发送消息
     */
    public static void sendMsgByAsync()  {
        Customer gugu = new Customer(1, "gugu");

        ProducerRecord producerRecord = new ProducerRecord("my-topic", gugu);
        try {
            Object o = producer.send(producerRecord).get();
            System.out.println(o);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        System.out.println("message send");
    }

    public static void sendMsgBySync()  {
        Customer gugu = new Customer(1, "gugu");
        ProducerRecord producerRecord = new ProducerRecord("my-topic", gugu);
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null){
                exception.printStackTrace();
            } else {
                System.out.println(" send success ");
            }
        });
    }


}
