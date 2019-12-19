package com.gugu.spark.day07

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import sparkSession.implicits._
    val lines: Dataset[String] = sparkSession.createDataset(List("1,laozhoa,china","2,laoduan,usa","3,laoyang,jp"))
    //对数据进行整理
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val nationCode: String = fields(2)
      (id, name, nationCode)
    })
    val df1: DataFrame = tpDs.toDF("id","name","nation")
    val nations: Dataset[String] = sparkSession.createDataset(List("china,中国","usa,美国"))
    //对数据进行整理
    val ndataset: Dataset[(String, String)] = nations.map(nation => {
      val fields: Array[String] = nation.split(",")
      val ename: String = fields(0)
      val cname: String = fields(1)
      (ename, cname)
    })
    val df2: DataFrame = ndataset.toDF("ename","cname")
    df2.count()

    //第一种，创建视图
    df1.createOrReplaceTempView("v_users")
    df2.createOrReplaceTempView("v_nations")
    val result: DataFrame = sparkSession.sql("SELECT name, cname FROM v_users JOIN v_nations ON nation = ename")
    result.show()
    import org.apache.spark.sql.functions._
    val result2: DataFrame = df1.join(df2, $"nation" === $"ename", "left_outer")
    result2.show()
  }
}
