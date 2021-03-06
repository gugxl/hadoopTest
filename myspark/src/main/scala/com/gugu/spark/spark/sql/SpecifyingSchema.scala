package com.gugu.spark.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SpecifyingSchema {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SpecifyingSchema").setMaster("local[*]")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)
    //从指定的地址创建RDD
    val personRDD: RDD[Array[String]] = sc.textFile(args(0)).map(_.split(" "))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    //将RDD映射到rowRDD
    val rowRDD: RDD[Row] = personRDD.map(p => Row(p(0).toInt,p(1),p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame: DataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //注册表
    personDataFrame.registerTempTable("t_person")
    //执行SQL
    val df: DataFrame = sqlContext.sql("select * from t_person order by age desc limit 4")
    //将结果以JSON的方式存储到指定位置
    df.write.json(args(1))
    //停止Spark Context
    sc.stop()
  }
}
