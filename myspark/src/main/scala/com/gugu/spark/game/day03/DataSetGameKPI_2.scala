package com.gugu.spark.game.day03

import com.gugu.spark.game.TimeUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetGameKPI_2 {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-01 00:00:00"
    val beginTime = TimeUtils(queryTime)
    //2016-02-03 00:00:00
    val endTime = TimeUtils.getCertainDayTime(3)

    val sparkSession: SparkSession = SparkSession.builder().appName("DataSetGameKPI_2").master("local[*]").getOrCreate()

    import sparkSession.implicits._
    //读取数据
    val lines: Dataset[String] = sparkSession.read.textFile(args(0))
    val filterUtils = new FilterUtils_V2
    //切分数据
    val splited: Dataset[Array[String]] = lines.map(_.split("[|]"))
    //过滤数据
    val filtered: Dataset[Array[String]] = splited.filter(fields => {
      filterUtils.filterByTime(fields, beginTime, endTime)
    })
    val updateUser: Dataset[Array[String]] = filtered.filter(fields => {
      filterUtils.filterByType(fields, EventType.REGISTER, EventType.UPGRAND)
    })
    val updateUserDF: DataFrame = updateUser.map(fields => {
      (fields(3), fields(6).toDouble)
    }).toDF("username", "level")
    updateUserDF.createOrReplaceTempView("v_update_user")
    //写SparkSQL
    val maxLevelDF: DataFrame = sparkSession.sql("select username , MAX(level) max_level from v_update_user group by username ")
    maxLevelDF.createOrReplaceTempView("v_max_level")
    val result: DataFrame = sparkSession.sql("SELECT AVG(max_level) from v_max_level")
    result.show()
    sparkSession.close()
  }
}
