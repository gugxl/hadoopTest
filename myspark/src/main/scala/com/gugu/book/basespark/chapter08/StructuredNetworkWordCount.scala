package com.gugu.book.basespark.chapter08

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object StructuredNetworkWordCount {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("StructuredNetworkWordCount").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        val lines: DataFrame = spark
            .readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
        import spark.implicits._
        val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
        val wordCounts: DataFrame = words.groupBy("value").count()
        val query: StreamingQuery = wordCounts
            .writeStream
            .outputMode("complete")
            .format("console")
            .trigger(Trigger.ProcessingTime("8 second"))
            .option("checkpointLocation", "file:///D:\\applicationfiles\\data\\kafka")
            .start()
        
        query.awaitTermination()
    }
    
}
