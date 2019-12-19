package com.gugu.spark.day08

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    //如果想让hive运行在spark上，一定要开启spark对hive的支持
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    //想要使用hive的元数据库，必须指定hive元数据的位置，添加一个hive-site.xml到当前程序的classpath下即可
    //有t_boy真个表或试图吗？
    //val result: DataFrame = spark.sql("SELECT * FROM t_boy ORDER BY fv DESC")
    val sql: DataFrame = sparkSession.sql("CREATE TABLE t_niu (id bigint, name string)")
    sql.show()
    sparkSession.stop()
  }
}
