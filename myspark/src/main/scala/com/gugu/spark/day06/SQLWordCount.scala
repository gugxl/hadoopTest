package com.gugu.spark.day06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLWordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()
    //(指定以后从哪里)读数据，是lazy

    //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //dataset只有一列，默认这列叫value
    val lines: Dataset[String] = sparkSession.read.textFile("D:\\logs\\wc")
    import sparkSession.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    //注册视图
    words.createOrReplaceTempView("v_wc")
    //执行SQL（Transformation，lazy）
    val result: DataFrame = sparkSession.sql("select value , count(*) as count from v_wc group by value order by count desc")
    //执行Action
    result.show()
    sparkSession.stop()
  }
}
