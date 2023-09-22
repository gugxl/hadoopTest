package com.gugu.book.basespark.chapter07

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object QueueStream {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(1))
        val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]
        val queueStream: InputDStream[Int] = ssc.queueStream(rddQueue)
        val mappedStream: DStream[(Int, Int)] = queueStream.map(x => (x % 10, 1))
        val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
        reducedStream.print()
        ssc.start()
        
        for (i <- 1 to 10) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 100, 2)
            Thread.sleep(1000)
        }
        
        ssc.stop()
        
    }
    
}
