package com.gugu.spark.day08

import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinTest {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import sparkSession.implicits._
//    import org.apache.spark.sql.functions._
//    sparkSession.sql.autoBroadcastJoinThreshold=-1
    sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    sparkSession.conf.set("spark.sql.join.preferSortMergeJoin", true)

    println(sparkSession.conf.get("spark.sql.autoBroadcastJoinThreshold"))
    val df1: DataFrame = Seq(
      (0, "playing"),
      (1, "with"),
      (2, "join")
    ).toDF("id", "token")
    val df2: DataFrame = Seq(
      (0, "P"),
      (1, "W"),
      (2, "S")
    ).toDF("aid", "atoken")
    df2.repartition()
    df1.cache().count()

    val result: DataFrame = df1.join(df2,$"id"===$"aid")
    //查看执行计划
    result.explain()
    result.show()
    sparkSession.stop()
  }
}
