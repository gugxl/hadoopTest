package com.gugu.spark.day07

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvDataSource {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("CsvDataSource").master("local[*]").getOrCreate()
    //指定以后读取json类型的数据
    val csv: DataFrame = sparkSession.read.csv("D:\\logs\\csv")
    csv.printSchema()
    val pdf: DataFrame = csv.toDF("id","name","age")
    pdf.show()
    sparkSession.stop()
  }
}
