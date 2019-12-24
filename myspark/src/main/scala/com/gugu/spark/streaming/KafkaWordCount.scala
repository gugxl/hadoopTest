package com.gugu.spark.streaming

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    // zk集群
    val topics = Array("my-topic")
    val preferConsistent: LocationStrategy = LocationStrategies.PreferConsistent
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "master:2181,slave1:2181,slave2:2181",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val data = KafkaUtils.createDirectStream(ssc,
      preferConsistent,
      Subscribe[String,String](topics, kafkaParams)
      )
    val lines: DStream[String] = data.map(_.value())
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordsAndOne: DStream[(String, Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = wordsAndOne.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
