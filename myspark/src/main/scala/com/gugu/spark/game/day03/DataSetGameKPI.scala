package com.gugu.spark.game.day03

import com.gugu.spark.game.TimeUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetGameKPI {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-01 00:00:00"
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(1)

    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("DataSetGameKPI").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    //读取数据
    val lines: Dataset[String] = sparkSession.read.textFile(args(0))
    val filterUtils = new  FilterUtils_V2
    //切分数据
    val splited: Dataset[Array[String]] = lines.map(_.split("[|]"))
    //过滤数据
    val filted: Dataset[Array[String]] = splited.filter(fields => {
      filterUtils.filterByTime(fields, beginTime, endTime)
    })

    //计算新增
    val dnuFf: DataFrame = filted.filter(fields => {
      filterUtils.filterByType(fields, EventType.REGISTER)
    }).map(fields => {
      //转换成元组
      //转换成Row
      //关联Scheama（定义CaseClass、StructType）
      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8))
    }).toDF("type", "time", "ip", "username", "job", "gender", "level", "money", "gold")
    dnuFf.createOrReplaceTempView("dnu")
    val dnu: Long = dnuFf.count()

    //时间起始时间
    //2016-02-02
    val t1 = endTime
    //2016-02-03
    val t2: Long = TimeUtils.getCertainDayTime(2)
    val nextDayLoginDf: DataFrame = splited.filter(fields => {
      filterUtils.filterByTime(fields, t1, t2)
    }).map(fields => {
      //转换成元组
      //转换成Row
      //关联Scheama（定义CaseClass、StructType）
      fields(3)
    }).distinct().toDF("username")
    nextDayLoginDf.createOrReplaceTempView("next_day_login")
    val result: DataFrame = sparkSession.sql("SELECT count(*) from dnu join next_day_login on (dnu.username = next_day_login.username)")
    result.show()
    sparkSession.stop()
  }
}
