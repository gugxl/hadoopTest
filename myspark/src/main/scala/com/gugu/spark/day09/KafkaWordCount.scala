package com.gugu.spark.day09

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka010}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val brokerList = "master:9092,slave1:9092,slave2:9092"
    val groupId = "g1"
    val topic = Set("my-topic")

    //准备kafka的参数
    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> brokerList,
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id" -> groupId,
      //      //从头开始读取数据
      //      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
      "auto.offset.reset"-> "latest",
      "enable.auto.commit"->(false: java.lang.Boolean)
    )
    //创建DStream，需要KafkaDStream
//    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topic)
    val data = KafkaUtils.createDirectStream(ssc,PreferConsistent,ConsumerStrategies.Subscribe[String,String](topic,kafkaParams))
    //对数据进行处理
    //Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_.value())
    //对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和一组合在一起
    val wordsAndOne: DStream[(String, Int)] = words.map((_,1))
    //聚合
    val reduced: DStream[(String, Int)] = wordsAndOne.reduceByKey(_+_)
    //打印结果(Action)
    reduced.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
