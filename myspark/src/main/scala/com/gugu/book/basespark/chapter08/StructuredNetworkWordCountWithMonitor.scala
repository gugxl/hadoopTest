package com.gugu.book.basespark.chapter08

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredNetworkWordCountWithMonitor {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("StructuredNetworkWordCountWithMonitor").master("local[*]").getOrCreate()
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
            .queryName("write_to_console")
            .trigger(Trigger.ProcessingTime("8 second"))
            .option("checkpointLocation", "file:///D:\\applicationfiles\\data\\ck6")
            .start()
        
        while (true) {
            if (query.lastProgress != null){
                if (query.lastProgress.numInputRows > 0){
                    println(query.lastProgress)
                }
            }
            println(query.status)
            Thread.sleep(500)
        }
    }
    
}
