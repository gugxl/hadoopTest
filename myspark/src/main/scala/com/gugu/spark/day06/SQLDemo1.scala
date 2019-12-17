package com.gugu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo1 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("SQLDemo1").master("local[*]").getOrCreate()

    val lines: RDD[String] = sparkSession.sparkContext.textFile("hdfs://master:9000/test/person/")
    //将数据进行整理
    val boyRDD: RDD[Boy] = lines.map(
      line => {
        val fields: Array[String] = line.split(" ")
        val id = fields(0).toInt
        val name = fields(1)
        val age = fields(2).toInt
        Boy(id, name, age)
      }
    )

    //该RDD装的是Boy类型的数据，有了shcma信息，但是还是一个RDD
    //将RDD转换成DataFrame
    //导入隐式转换
    import sparkSession.sqlContext.implicits._
    val boyDF: DataFrame = boyRDD.toDF
    //变成DF后就可以使用两种API进行编程了
    //把DataFrame先注册临时表
    boyDF.createOrReplaceTempView("t_boy")
    //书写SQL（SQL方法应其实是Transformation）
    val reslt: DataFrame = sparkSession.sqlContext.sql("select * from t_boy order by age desc ")
    //查看结果（触发Action）
    reslt.show()
    sparkSession.stop()
  }
}
case class Boy(id:Int,name:String,age:Int)