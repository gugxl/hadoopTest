package com.gugu.book.basespark.chapter08

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaDataProducer {
    def main(args: Array[String]): Unit = {
        val properties = new Properties()
        properties.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](properties)
        while (true) {
            var lowercase: Array[Char] = new Array[Char](0)
            for (i <- 'a' to 'z') {
                lowercase = lowercase ++ Array(i)
            }
            val str = util.Random.shuffle(lowercase.toList).take(2)
            val word = new StringBuffer
            for (j <- str) {
                word.append(j)
            }
            val message = new ProducerRecord[String, String]("wordcount", word.toString)
            producer.send(message)
            Thread.sleep(100)
        }
    }
    
}
