package com.gugu.spark.game.day03

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKPI_V1 {
  def main(args: Array[String]): Unit = {
    //条件的SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startTime: Long = dateFormat.parse("2016-02-01").getTime
    val endTime: Long = dateFormat.parse("2016-02-01").getTime
    //如果在Driver端new出一个SimpleDateFormat实例，每个Task内部都会一个独立的SimpleDateFormat实例
    //如何用一个SimpleDateFormat实例？定义一个object类型的工具类，让SimpleDateFormat作为一个成员变量
    val sdf = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
    val conf: SparkConf = new SparkConf().setAppName("GameKPI_V1").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val lines: RDD[String] = sparkContext.textFile("D:\\logs\\game\\")
    

  }
}
