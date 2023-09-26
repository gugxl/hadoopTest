package com.gugu.book.basespark.chapter09

import org.apache.spark.sql.SparkSession

object Demo05 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo05").master("local").getOrCreate()
        val df = spark.read.format("libsvm")
            .option("numFeatures", "780")
            .load("file:///D:\\applicationfiles\\data\\sample_libsvm_data.txt")
        df.show()
    }
    
}
