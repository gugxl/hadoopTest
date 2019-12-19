package com.gugu.spark.game

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKPI {
  def main(args: Array[String]): Unit = {
    //"2016-02-01"
    val startDate = args(0)
    //"2016-02-02"
    val endDate = args(1)
    //查询条件
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //查寻条件的的起始时间
    val startTime: Long = dateFormat.parse(startDate).getTime
    //查寻条件的的截止时间
    val endTime: Long = dateFormat.parse(endDate).getTime

    //Driver定义的一个simpledataformat
    val dateFormat2 = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
    val conf: SparkConf = new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    //以后从哪里读取数据
    val lines: RDD[String] = sparkContext.textFile(args(2))
    val spliterRDD: RDD[Array[String]] = lines.map(_.split("[|]"))

    val filterd: RDD[Array[String]] = spliterRDD.filter(fields => {
      val t: String = fields(0)
      val time: String = fields(1)
      val timeLong: Long = dateFormat2.parse(time).getTime
      "1".equals(t) && timeLong >= startTime && timeLong < endTime
    })
    val dnu: Long = filterd.count()
    println(dnu)
    sparkContext.stop()
  }
}
