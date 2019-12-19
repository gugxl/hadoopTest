package com.gugu.spark.day07

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDataSource {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("ParquetDataSource").master("local[*]").getOrCreate()
    val parquetLine: DataFrame = sparkSession.read.parquet("hdfs://master:9000/test/parquet2/part-00000-0c4b7561-b1f5-42ca-a351-161e6f942c5f-c000.snappy.parquet")
//    sparkSession.read.format("parquet").load("hdfs://master:9000/test/parquet2/part-00000-0c4b7561-b1f5-42ca-a351-161e6f942c5f-c000.snappy.parquet").
    parquetLine.printSchema()
    parquetLine.show()
    sparkSession.stop()
  }
}
