package com.gugu.spark.day06

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
      val spark: SparkSession = SparkSession.builder()
        .appName("DataSetWordCount").master("local[*]").getOrCreate()
    //(指定以后从哪里)读数据，是lazy
    //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //dataset只有一列，默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("hdfs://master:8020/wc/input/")
    //整理数据(切分压平)
    //导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //使用DataSet的API（DSL）
//    val count: DataFrame = words.groupBy($"value" as "word").count().sort($"count" desc)

    //导入聚合函数
    import org.apache.spark.sql.functions._
    val counts: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)
    counts.show()
    spark.stop()
  }
}
