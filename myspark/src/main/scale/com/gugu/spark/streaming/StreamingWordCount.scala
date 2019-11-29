package com.gugu.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    // 离线任务是创建sparkContext,要实现实时计算需要用StreamingContext
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 对sparkContext进行了包装，增强了实时的功能
    // 第二个参数是小批次产生的时间间隔
    val ssc = new StreamingContext(sc, Milliseconds(5000))
    //从socket端口中读数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.2.50",8889)
    // 对DStream进行操作
    val words: DStream[String] = lines.flatMap(_.split(" "))
    // 组合
    val wordsAndOne: DStream[(String, Int)] = words.map((_, 1))
    // 聚合
    val reduced: DStream[(String, Int)] = wordsAndOne.reduceByKey(_+_)
    // 打印 Action
    reduced.print()

    // 启动Streaming程序
    ssc.start()

    // 等待优雅退出[将正在执行的任务处理完,接收信息的任务]
    ssc.awaitTermination()

  }
}
