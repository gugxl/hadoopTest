package com.gugu.spark.game

import java.text.SimpleDateFormat
import java.util.Calendar

object TimeUtils {
  private val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val calendar: Calendar = Calendar.getInstance()

  def apply(time: String):Long = {
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }

  def getCertainDayTime(amount : Int) ={
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amount)
    time
  }
}
