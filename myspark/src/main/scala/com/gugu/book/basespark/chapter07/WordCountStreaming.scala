package com.gugu.book.basespark.chapter07

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountStreaming {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(20))
        val lines: DStream[String] = ssc.textFileStream("file:///D:\\applicationfiles\\data\\log\\")
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val wordsAndCount: DStream[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)
        wordsAndCount.print()
        ssc.start()
        ssc.awaitTermination()
    }
    
}
