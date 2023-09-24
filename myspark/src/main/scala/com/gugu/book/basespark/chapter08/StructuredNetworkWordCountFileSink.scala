package com.gugu.book.basespark.chapter08

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredNetworkWordCountFileSink {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("StructuredRate")
            .master("local").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        val lines: DataFrame = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
        
        import spark.implicits._
        val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
        val length5word: Dataset[String] = words.filter(_.length == 5)
        val query: StreamingQuery = length5word.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", "file:///D:\\applicationfiles\\data\\fileshink")
            .option("checkpointLocation", "file:///D:\\applicationfiles\\data\\ck5")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .start()
        
        query.awaitTermination()
        
    }
}