package com.gugu.book.basespark.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Demo01 {
    
    def readParquet(spark: SparkSession): Unit = {
        val path = "/data/tmp/wc/parquet/part-00000-2bfa1ea6-1798-4510-8e8f-ef71fd99e6ca-c000.snappy.parquet"
        // 下面两种写法等价
        val df: DataFrame = spark.read.parquet(path)
        df.show()
        
        val df2: DataFrame = spark.read.format("parquet").load(path)
        df2.show()
        
    }
    
    def writeParquet(spark: SparkSession): Unit = {
        val bookDF: DataFrame = spark.createDataFrame(Array(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)))
            .toDF("book", "amount")
        //        下面两种写法是等价的
        bookDF.write.parquet("/data/tmp/wc/parquet")
        bookDF.write.format("parquet")
            .mode("overwrite")
            .option("compression", "snappy")
            .save("/data/tmp/wc/parquet")
    }
    
    def readJson(spark: SparkSession): Unit = {
        val path = "file:///D:\\applicationfiles\\data\\people.json"
        spark.read.format("json").load(path).show()
        spark.read.json(path).show()
    }
    
    def writeJson(spark: SparkSession): Unit = {
        val bookDF: DataFrame = spark.createDataFrame(Array(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)))
            .toDF("book", "amount")
        bookDF.write.json("/data/tmp/wc/json")
        bookDF.write.format("json").save("/data/tmp/wc/json")
    }
    
    def readText(spark: SparkSession): Unit = {
        
        val path = "file:///D:\\applicationfiles\\data\\people.json"
        val df: DataFrame = spark.read.text(path)
        val df1: DataFrame = spark.read.format("text").load(path)
        df.show()
        df1.show()
        
    }
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo01").master("local").getOrCreate()
        //        rddGetAvg(spark)
        //        sparkSqlGetAvg(spark)
        //        readParquet(spark)
//        writeParquet(spark)
//        readJson(spark)
//        writeJson(spark)
        readText(spark)
        spark.stop
    }
    
    
    def rddGetAvg(session: SparkSession): Unit = {
        //        RDD 方式聚合数据，会导致代码逻辑比较难懂
        val bookRDD: RDD[(String, Int)] = session.sparkContext.parallelize(Array(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)))
        val saleRDD: Array[(String, Int)] = bookRDD.map(x => (x._1, (x._2, 1)))
            .reduceByKey((x, y) => (x._1 + y._1, x._2 + x._2))
            .map(x => (x._1, x._2._1 / x._2._2))
            .collect()
        saleRDD.foreach(println(_))
    }
    
    def sparkSqlGetAvg(session: SparkSession): Unit = {
        // 直接使用sparkSQL中的聚合函数， DataFrame包含数据格式的RDD
        val bookDF: DataFrame = session.createDataFrame(Array(("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)))
            .toDF("book", "amount")
        val avgDF: DataFrame = bookDF.groupBy("book").agg(avg("amount"))
        avgDF.show()
    }
    
}
