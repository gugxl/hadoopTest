package com.gugu.book.basespark.chapter03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("hello world").setMaster("local[*]")
        val context = new SparkContext(conf)
        val fileRDD: RDD[String] = context.textFile("/data/tmp/wc/input")
        val l: Long = fileRDD.filter(_.contains("a")).count()
        println(l)
    }
    
}
