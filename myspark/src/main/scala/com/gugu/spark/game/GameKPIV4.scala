package com.gugu.spark.game

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GameKPIV4 {
  def main(args: Array[String]): Unit = {
    //"2016-02-01"
    val startDate = args(0)
    //"2016-02-02"
    val endDate = args(1)

    val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd")

    val startTime = dateFormat1.parse(startDate).getTime

    val endTime = dateFormat1.parse(endDate).getTime


    val conf = new SparkConf().setAppName("GameKPIV2").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //以后从哪里读取数据
    val lines: RDD[String] = sc.textFile(args(2))

    //整理并过滤
    val splited: RDD[Array[String]] = lines.map(line => line.split("[|]"))
    //FilterUtils是在Driver端创建的
    //    val filteredByType = splited.filter(fields => {
    //      fu.filterByType(fields, "1")
    //    })

    val filtered: RDD[Array[String]] = splited.filter(FilterUtilsV4.filterByTime(_, startTime, endTime))
    val dnu = filtered.count()

    println(dnu)

    sc.stop()
  }
}
