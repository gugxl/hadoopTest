package com.gugu.book.basespark.chapter08

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaDataConsumer {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("KafkaDataConsumer").master("local").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val kafkaServer = "master:9092,slave1:9092,slave2:9092"
        val lines: DataFrame = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServer)
            .option("subscribe", "wordcount")
            .load()
            .selectExpr("CAST(value AS STRING)")
        
        val wordCounts: DataFrame = lines.groupBy("value").count()
        
        
        val query: StreamingQuery = wordCounts
            .selectExpr("CAST(value AS STRING) as key",
                "CONCAT(CAST(value AS STRING),':', CAST(count AS STRING)) as value")
            .writeStream
            .outputMode("complete")
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServer)
            .option("topic", "wordcount")
            .option("checkpointLocation", "file:///D:\\applicationfiles\\data\\ck3")
            .trigger(Trigger.ProcessingTime("8 seconds"))
            .start()
        
        query.awaitTermination()
    }
    
}
