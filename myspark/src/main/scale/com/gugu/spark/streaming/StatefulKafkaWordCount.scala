//package com.gugu.spark.streaming
//
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
//import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//
//
//object StatefulKafkaWordCount {
//
//  /**
//   * 第一个参数，聚合的key
//   * 第二个参数，当前批次该单词在每一个分区出现的次数
//   *  第三个参数，初始化值或中间累加的结果
//   */
//  val updateFunc = (time: Time,iter:Iterator[(String,Seq[Int], Option[Int])]) => {
//    iter.map{
//      case (x,y,z) => (x, y.sum + z.getOrElse(0))
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, Seconds(10))
//    // zk集群
//    val zkCluster = "master:2181,slave1:2181,slave2:2181"
//    val topic= Map("my-topic" -> 1)
//    val groupId = "g1"
//    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkCluster, groupId, topic)
//    val lines: DStream[String] = data.map(_._2)
//    val words: DStream[String] = lines.flatMap(_.split(" "))
//    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
//    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
//    result.print()
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//
//
//
//}
