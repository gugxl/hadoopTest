package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MaxAndMin {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MaxAndMin").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val lines: RDD[String] = sc.textFile("file:///D:\\applicationfiles\\data\\top", 2)
        lines.filter(_.trim().length > 0)
            .map(line => ("key", line.split(",")(2).trim.toInt))
            .groupByKey()
            .map(x => {
                var min = Integer.MAX_VALUE
                var max = Integer.MIN_VALUE
                for (num <- x._2) {
                    if (num > max) {
                        max = num
                    }
                    
                    if (num < min) {
                        min = num
                    }
                }
                (max, min)
            }).collect().foreach(x => {
            println("max =" + x._1)
            println("min =" + x._2)
        })
        
        
    }
}
