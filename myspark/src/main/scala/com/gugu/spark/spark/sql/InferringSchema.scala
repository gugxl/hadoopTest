package com.gugu.spark.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object InferringSchema {
  //2.1.1.通过反射推断Schema
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("InferringSchema").setMaster("local[*]")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)
    //从指定的地址创建RDD
    val lineRDD: RDD[Array[String]] = sc.textFile(args(0)).map(_.split(" "))
    //创建case class
    //将RDD和case class关联
    val personRDD: RDD[Person] = lineRDD.map(m => Person(m(0).toInt,m(1),m(2).toInt))
    //导入隐式转换，如果不到人无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF: DataFrame = personRDD.toDF()
    //注册表
    personDF.registerTempTable("t_person")
    //传入SQL
    val df: DataFrame = sqlContext.sql("select * from t_person order by age desc limit 2")
    //将结果以JSON的方式存储到指定位置
    df.write.json(args(1))
    //停止Spark Context
    sc.stop()
  }
}
//case class一定要放到外面
case class Person(id:Int,name:String,age:Int)
