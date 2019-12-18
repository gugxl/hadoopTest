package com.gugu.spark.game.day03

import org.apache.commons.lang3.time.FastDateFormat

class FilterUtils {
  //SimpleDateFormat是线程不安全的
  //val dateFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")

  //线程安全的DateFormat
  private val dataFormate: FastDateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")
  def filterByTime(fields: Array[String], startTime: Long, endTime:Long) = {
    val time = fields(1)
    val logTime: Long = dataFormate.parse(time).getTime
    logTime >= startTime && logTime < endTime
  }
  def filterByType(fields: Array[String],evenType:String) ={
    val _type: String = fields(0)
    evenType == _type
  }
  def filterByTypes(fields: Array[String],evenTypes:String*): AnyVal ={
    val _type: String = fields(0)
    for(evenType <- evenTypes){
      if(evenType == _type){
        return true
      }
    }
    false
  }
  def filterByTypeAndTime(fields:Array[String], eventType: String, beginTime: Long, endTime: Long): Boolean = {
    val _type: String = fields(0)
    val _time = fields(1)
    val logTime = dataFormate.parse(_time).getTime
    eventType == _type && logTime >= beginTime && logTime < endTime
  }
}
