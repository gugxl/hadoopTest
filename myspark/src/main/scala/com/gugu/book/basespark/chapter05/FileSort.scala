package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object FileSort {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FileSort").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val lines: RDD[String] = sc.textFile("file:///D:\\applicationfiles\\data\\filesort")
        
        var index = 0
        val result: RDD[(Int, Int)] = lines.filter(_.length > 0)
            .map(n => (n.trim.toInt, ""))
            .partitionBy(new HashPartitioner(1))
            .sortByKey()
            .map(t => {
                index += 1
                (index, t._1)
            })
        
        result.collect().foreach(x => println(x._1 + "\t" + x._2))
        
    }
    
}
