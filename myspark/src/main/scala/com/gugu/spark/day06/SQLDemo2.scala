package com.gugu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SQLDemo2 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("SQLDemo2").master("local[*]").getOrCreate()
    val lines = sparkSession.sparkContext.textFile("D:\\logs\\person\\")
    val personRDD: RDD[Row] = lines.map(
      line => {
        val fields: Array[String] = line.split(" ")
        val id: Int = fields(0).toInt
        val name: String = fields(1)
        val age: Int = fields(2).toInt
        Row(id, name, age)
      }
    )
    //结果类型，其实就是表头，用于描述DataFrame

    val sch = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    //将RowRDD关联schema
    val personDataFrame: DataFrame = sparkSession.sqlContext.createDataFrame(personRDD, sch)
    personDataFrame.createOrReplaceTempView("t_person")
    val result: DataFrame = sparkSession.sqlContext.sql("select * FROM t_person order by age")
    result.show()

    sparkSession.stop()
  }
}
