package com.gugu.book.basespark.chapter09

import org.apache.spark.sql.SparkSession


object Demo04 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo04").master("local").getOrCreate()
        val df = spark.read.format("image").load("/gugu/kittens/")
        
        df.select("image.origin", "image.width", "image.height").show(truncate = false)
    }
    
}
