package com.gugu.spark.game.day03

import java.text.SimpleDateFormat

class FilterUtils_V2 extends Serializable {
  //SimpleDateFormat是线程不安全的
  private val simpleDateFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")

  def filterByTime(fields: Array[String], startTime:Long, endTime:Long): Boolean = {
    val time = fields(1)
    val longTime: Long = simpleDateFormat.parse(time).getTime
    longTime >= startTime && longTime <= endTime
  }

  def filterByType(fields: Array[String], eventTypes: String*): Boolean ={
    val _type = fields(0)
    for(et <- eventTypes){
      if(_type == et){
        return true
      }
    }
    false
  }

  def filterByTypeAndTime(fields: Array[String], eventTypes: String,beginTime: Long, endTime: Long) ={
    val _type = fields(0)
    val _time = fields(1)
    val longTime = simpleDateFormat.parse(_time).getTime
    eventTypes == _type && longTime >= beginTime && longTime < endTime
  }
}
