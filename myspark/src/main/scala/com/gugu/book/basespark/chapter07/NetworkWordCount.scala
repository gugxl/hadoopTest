package com.gugu.book.basespark.chapter07

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(1))
        if (args.length != 2) {
            System.err.println("Usage: NetworkWordCount <hostname> <port>")
            System.exit(1)
        }
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream(args(0), args(1).toInt)
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val wordCounts: DStream[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)
        wordCounts.print()
        ssc.start()
        ssc.awaitTermination()
    }
    
}
