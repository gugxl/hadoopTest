package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
    override def compare(that: SecondarySortKey): Int = {
        
        if (this.first - that.first != 0) {
            return this.first - that.first
        } else {
            return this.second - that.second
        }
    }
}

object SecondarySortApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SecondarySortApp").setMaster("local")
        val sc = new SparkContext(conf)
        val lines: RDD[String] = sc.textFile("file:///D:\\applicationfiles\\data\\SecondarySortKey")
        val pairWithSortKey: RDD[(SecondarySortKey, String)] = lines.map(line =>
            (new SecondarySortKey(line.split(" ")(0).toInt,
                line.split(" ")(1).toInt), line))
        
        pairWithSortKey.sortByKey(false)
            .map(_._2).collect().foreach(println(_))
        
        
    }
}