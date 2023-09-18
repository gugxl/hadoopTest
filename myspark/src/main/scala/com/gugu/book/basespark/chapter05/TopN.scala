package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("TopN").setMaster("local")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val lines: RDD[String] = sc.textFile("file:///D:\\applicationfiles\\data\\top")
        var num = 0
        lines.filter(line => (line.trim.length > 0) && (line.split(",").length == 4))
            .map(_.split(",")(2))
            .map(x => (x.trim.toInt, ""))
            .sortByKey(false)
            .map(x => x._1)
            .take(5)
            .foreach(x => {
                num += 1
                println(num + "\t" + x)
            })
    }
    
}
