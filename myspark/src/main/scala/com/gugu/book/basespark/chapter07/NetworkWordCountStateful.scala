package com.gugu.book.basespark.chapter07

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object NetworkWordCountStateful {
    
    def main(args: Array[String]): Unit = {
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.foldLeft(0)(_ + _)
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
        }
        
        val conf: SparkConf = new SparkConf().setAppName("NetworkWordCountStateful").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(5))
        ssc.checkpoint("file:///D:\\applicationfiles\\data\\kafka\\checkpoint")
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val wordDstream: DStream[(String, Int)] = words.map((_, 1))
        
        val stateDstream: DStream[(String, Int)] = wordDstream.updateStateByKey(updateFunc)
        stateDstream.print()
        ssc.start()
        ssc.awaitTermination()
        
        
        
    }
    
}
