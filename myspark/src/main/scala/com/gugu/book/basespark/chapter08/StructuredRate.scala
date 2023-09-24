package com.gugu.book.basespark.chapter08

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredRate {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("StructuredRate")
            .master("local").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
        val lines: DataFrame = spark.readStream
            .format("rate")
            .option("rowsPerSecond", 5)
            .load()
        println(lines.schema)
        val query: StreamingQuery = lines.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", false)
            .option("checkpointLocation", "file:///D:\\applicationfiles\\data\\ck4")
            .start()
        query.awaitTermination()
        
    }
}
