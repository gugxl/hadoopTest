package com.gugu.spark.game.day03

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKPI_V1 {
  def main(args: Array[String]): Unit = {
    //条件的SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startTime: Long = dateFormat.parse("2016-02-01").getTime
    val endTime: Long = dateFormat.parse("2016-02-02").getTime
    //如果在Driver端new出一个SimpleDateFormat实例，每个Task内部都会一个独立的SimpleDateFormat实例
    //如何用一个SimpleDateFormat实例？定义一个object类型的工具类，让SimpleDateFormat作为一个成员变量
    val sdf = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
    val conf: SparkConf = new SparkConf().setAppName("GameKPI_V1").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val lines: RDD[String] = sparkContext.textFile("D:\\logs\\game\\")
    val filteredData: RDD[String] = lines.filter(line => {
      val fields: Array[String] = line.split("[|]")
      val tp = fields(0)
      val timeStr: String = fields(1)
      //将字符串转换成Date
      val timeLong = sdf.parse(timeStr).getTime
      timeLong >= startTime && timeLong < endTime
    })

    val result: Array[String] = filteredData.collect()
    println(result.toBuffer)
    sparkContext.stop()
  }
}
