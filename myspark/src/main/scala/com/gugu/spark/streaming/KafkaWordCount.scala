package com.gugu.spark.streaming

import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    // zk集群
    val zkCluster = "master:2181,slave1:2181,slave2:2181"
    val topic= Map("my-topic" -> 1)
    val groupId = "g1"
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkCluster, groupId, topic)
    val lines: DStream[String] = data.map(_._2)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordsAndOne: DStream[(String, Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = wordsAndOne.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
