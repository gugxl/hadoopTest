package com.gugu.spark.spark.sql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object JdbcRDD {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("JdbcRDD").master("local[*]").getOrCreate()
    val personRDD = sparkSession.sparkContext.parallelize(Array("11 tom 5", "12 jerry 3", "13 kitty 6")).map(_.split(" "))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    //将RDD映射到rowRDD
    val rowRDD: RDD[Row] = personRDD.map(p => {
      Row(p(0).toInt, p(1), p(2).toInt)
    })

    //将schema信息应用到rowRDD上
    val personDaraFrame: DataFrame = sparkSession.sqlContext.createDataFrame(rowRDD, schema)
    //创建Properties存储数据库相关属性
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")
    properties.setProperty("user","root")
    //将数据追加到数据库
    personDaraFrame.write.mode("append").jdbc("jdbc:mysql://master:3306/bigdata","bigdata.t_person",properties)
    //停止SparkContext
    sparkSession.stop()
  }
}
