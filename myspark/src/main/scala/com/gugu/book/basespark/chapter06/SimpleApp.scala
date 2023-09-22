package com.gugu.book.basespark.chapter06

import org.apache.spark.sql.{Dataset, SparkSession}

object SimpleApp {
    def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SimpleApp").master("local").getOrCreate()
        val logData: Dataset[String] = spark.read.textFile("file:///D:\\applicationfiles\\data\\ml-25m\\README.txt")
        val acount: Long = logData.filter(line => line.contains("a")).count()
        val bcount: Long = logData.filter(line => line.contains("b")).count()
        println(acount + "\t" + bcount)
        spark.stop()

    }
    
}
