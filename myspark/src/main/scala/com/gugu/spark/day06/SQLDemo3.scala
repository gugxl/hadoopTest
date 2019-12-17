package com.gugu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLDemo3 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("SQLDemo3").master("local[*]").getOrCreate()
    val lines: RDD[String] = sparkSession.sparkContext.textFile("file:///D:\\logs\\person\\")
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      Row(id, name, age)
    })
    val scheama = StructType(
      List(
        StructField("id", LongType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    val personDF: DataFrame = sparkSession.sqlContext.createDataFrame(rowRDD, scheama)
    //不使用SQL的方式，就不用注册临时表了
    val df1: DataFrame = personDF.select("name","age")
    import sparkSession.sqlContext.implicits._
    val df2: Dataset[Row] = df1.orderBy($"age" desc, $"id" asc)
    df2.show()
    sparkSession.stop()
  }
}
