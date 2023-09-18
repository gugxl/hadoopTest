package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object JSONRead {
    def main(args: Array[String]): Unit = {
        val path = "file:///D:\\applicationfiles\\data\\people.json"
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JSONRead")
        val sc = new SparkContext(conf)
        val jsonStrs: RDD[String] = sc.textFile(path)
        val result: RDD[Option[Any]] = jsonStrs.map(JSON.parseFull(_))
        result.foreach({ r =>
            r match {
                case Some(map: Map[String, Any]) => println(map)
                case None => println("Parsing failed")
                case other => println("Unknown data structure: " + other)
            }
        })
        
    }
    
}
