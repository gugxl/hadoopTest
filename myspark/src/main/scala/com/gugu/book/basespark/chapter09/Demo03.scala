package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.linalg.{Matrices, Matrix}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo03 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo03").master("local").getOrCreate()
//        val examples: DataFrame = spark.read
//            .format("libsvm")
//            .load("file:///D:\\applicationfiles\\data\\sample_libsvm_data.txt")
//
//        println(examples.collect().head)
        
        val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
        println(dm)
        val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
        println(sm)
    }
    
}
