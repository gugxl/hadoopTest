package com.gugu.spark.game.day03

import com.gugu.spark.game.TimeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GameKPI {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-02 00:00:00"
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(+1)
    val sparkSession: SparkSession = SparkSession.builder().appName("GameKPI").master("local[*]").getOrCreate()
    val filterUtils = new FilterUtils_V2
    //    //切分之后的数据
    val splitedLogs: RDD[Array[String]] = sparkSession.sparkContext.textFile("hdfs://master:8020/test/game/").map(_.split("[|]"))
    //    //过滤后并缓冲
    val filteredLogs: RDD[Array[String]] = splitedLogs.filter(fields => {
      filterUtils.filterByTime(fields, beginTime, endTime)
    })
    //日新增用户数，Daily New Users 缩写 DNU
    val dnu: Long = filteredLogs.filter(fileds => {
      filterUtils.filterByType(fileds, EventType.REGISTER)
    }).count()
    //日活跃用户数 DAU （Daily Active Users）
    val dau: Long = filteredLogs.filter(fields => {
      filterUtils.filterByType(fields, EventType.REGISTER, EventType.LOGIN)
    })
      .map(_ (3))
      .distinct()
      .count()
    println(s"日新增用户数: ${dnu}")
    println(s"日活跃用户数 ${dau}")
    //  留存率：某段时间的新增用户数记为A，经过一段时间后，仍然使用的用户占新增用户A的比例即为留存率
    //  次日留存率（Day 1 Retention Ratio） Retention [rɪ'tenʃ(ə)n] Ratio ['reɪʃɪəʊ]
    //  日新增用户在+1日登陆的用户占新增用户的比例
    val t1: Long = TimeUtils.getCertainDayTime(-1)
    val lastDayRegUser: RDD[(String, Int)] = splitedLogs.filter(fields => {
      filterUtils.filterByTypeAndTime(fields, EventType.REGISTER, t1, beginTime)
    }).map(x => (x(3), 1))
    val todayLoginUser: RDD[(String, Int)] = filteredLogs.filter(fields => {
      filterUtils.filterByType(fields, EventType.LOGIN)
    }).map(x => (x(3), 1)).distinct()
    val d1r: Long = lastDayRegUser.join(todayLoginUser).count()
    println("d1r:"+d1r)
    val d1rr = d1r*1.0/ lastDayRegUser.count()
    println(d1rr)
    sparkSession.stop()
  }


}