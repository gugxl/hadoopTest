package com.gugu.spark.game

import java.text.SimpleDateFormat

class FilterUtilsV3 {
  //如果object使用了成员变量，那么会出现线程安全问题，因为object是一个单例，多线程可以同时调用这个方法
  private val dateFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
  def filterByType(fields:Array[String], tp:String) = {
    val _tp: String = fields(0)
    tp == _tp
  }
  def filterByTime(fields:Array[String], startTime:Long, endTime:Long) = {
    val timeStr: String = fields(1)
    val time: Long = dateFormat.parse(timeStr).getTime
    time >= startTime && time < endTime
  }

}
