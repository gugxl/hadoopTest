package com.gugu.book.basespark.chapter07

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaWordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(10))
        ssc.checkpoint("file:///D:\\applicationfiles\\data\\kafka\\checkpoint")
        
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "use_a_separate_group_id_for_each_stream",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )
        
        val topics = Array("wordsender")
        val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams))
        
        stream.foreachRDD(rdd => {
            val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val maped: RDD[(String, String)] = rdd.map(recoder => (recoder.key(), recoder.value()))
            
            val lines: RDD[String] = maped.map(_._2)
            val words: RDD[String] = lines.flatMap(_.split(" "))
            val wordsAndOne: RDD[(String, Int)] = words.map((_, 1))
            val wordCounts: RDD[(String, Int)] = wordsAndOne.reduceByKey(_ + _)
            wordCounts.foreach(println)
        })
        
        ssc.start()
        ssc.awaitTermination()
        
    }
    
}
