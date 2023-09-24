package com.gugu.book.basespark.chapter08

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
object StructuredEMallPurchaseCount {
    // 定义JSON文件的路径常量
    val TEST_DATA_DIR_SPARK = "file:///D:\\applicationfiles\\data\\tmp"
    
    def main(args: Array[String]): Unit = {
        // 定义模式，为时间戳类型的eventTime、字符串类型的操作和省份组成
        val schema = new StructType(Array(
            StructField("eventTime", TimestampType, true),
            StructField("action", StringType, true),
            StructField("district", StringType, true)))
        val spark: SparkSession = SparkSession
            .builder
            .appName("StructuredEMallPurchaseCount")
            .master("local[*]")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val lines: DataFrame = spark
            .readStream
            .format("json")
            .schema(schema)
            .option("maxFilesPerTrigger", 5)
            .load(TEST_DATA_DIR_SPARK)
        
        import spark.implicits._
        val windowDuration = "1 minutes"
        
        val windowedCounts: Dataset[Row] = lines
            .filter($"action" === "purchase")
            .groupBy($"district", window($"eventTime", windowDuration))
            .count()
            .sort(asc("window"))
        
        val query: StreamingQuery = windowedCounts
            .writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("checkpointLocation", "file:///D:\\applicationfiles\\data\\ck2")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .start()
        
        query.awaitTermination()
    }
    
}
