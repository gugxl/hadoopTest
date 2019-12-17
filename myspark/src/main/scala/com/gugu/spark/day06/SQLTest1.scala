package com.gugu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLTest1 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("SQLTest1").master("local[*]").getOrCreate()
    val lines: RDD[String] = sparkSession.sparkContext.textFile("D:\\logs\\person\\")
    val personRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      val id: Int = fields(0).toInt
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      Row(id, name, age)
    })
    val schema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    val personDataFrame: DataFrame = sparkSession.sqlContext.createDataFrame(personRDD, schema)
    import sparkSession.implicits._
    val result: Dataset[Row] = personDataFrame.where($"age" > 18).orderBy($"age")
    result.show()
    sparkSession.stop()
  }
}
