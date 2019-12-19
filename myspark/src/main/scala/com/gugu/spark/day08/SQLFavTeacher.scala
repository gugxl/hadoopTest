package com.gugu.spark.day08

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLFavTeacher {
  def main(args: Array[String]): Unit = {
    val topN: Int = args(1).toInt
    val sparkSession: SparkSession = SparkSession.builder().appName("SQLFavTeacher").master("local[*]").getOrCreate()
    val lines: Dataset[String] = sparkSession.read.textFile(args(0))

    import sparkSession.implicits._
    val df: DataFrame = lines.map(line => {
      val tIndex: Int = line.lastIndexOf("/") + 1
      val teacher: String = line.substring(tIndex)
      val host: String = new URL(line).getHost
      //学科的index
      val sIndex: Int = host.indexOf(".")
      val subject: String = host.substring(0, sIndex)
      (subject, teacher)
    }).toDF("subject", "teacher")
    df.createOrReplaceTempView("v_sub_teacher")
    //该学科下的老师的访问次数
    val temp1: DataFrame = sparkSession.sql("SELECT subject, teacher, count(*) counts FROM v_sub_teacher GROUP BY subject, teacher")
    //求每个学科下最受欢迎的老师的topn
    temp1.createOrReplaceTempView("v_temp_sub_teacher_counts")

    val temp2: DataFrame = sparkSession.sql(s"select * from (" +
      s"SELECT subject, teacher,counts," +
      s"row_number() over (PARTITION BY SUBJECT ORDER BY counts desc) sub_rk, " +
      s"rank() over (ORDER BY counts desc) g_rk " +
      s"from v_temp_sub_teacher_counts" +
      s") temp2 where sub_rk <= ${topN}")
//    val temp2: DataFrame = sparkSession.sql(s"select *,row_number() over(order by counts desc) g_rk from (select subject, teacher,counts,row_number() over(PARTITION BY subject ORDER BY counts desc) sub_rk from v_temp_sub_teacher_counts) temp2 where sub_rk < ${topN}")
//      val temp2 = sparkSession.sql(s"select *, dense_rank() over(order by counts desc) g_rk from (select subject, teacher, counts, row_number() over(PARTITION BY subject order by counts desc) sub_rk from v_temp_sub_teacher_counts) temp2 where sub_rk<=$topN")

    temp2.show()
    sparkSession.stop()
  }
}
