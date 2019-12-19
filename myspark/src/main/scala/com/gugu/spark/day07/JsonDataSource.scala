package com.gugu.spark.day07

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object JsonDataSource {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("JsonDataSource").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    //指定以后读取json类型的数据(有表头)
    val jsons: DataFrame = sparkSession.read.json("D:\\logs\\json")
    val filtered: Dataset[Row] = jsons.where($"age"<=200)
    filtered.printSchema()
    filtered.show()
    sparkSession.stop()
  }
}
