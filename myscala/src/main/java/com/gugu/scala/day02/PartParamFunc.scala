package com.gugu.scala.day02

import java.util.Date

object PartParamFunc extends App {
  // 定义个输出的方法, 参数为date, message
  def log(date:Date, msg : String)={
    println(s"$date $msg")
  }
  val date = new Date()
  // 调用log的时候, 传递了一个具体的时间参数, message 为待定参数
  // logBoundDate 成了一个新的函数, 只有log的部分参数(message)
  val logBoundDate = log(date , _: String)
  // 调用logBoundDate的时候, 只需要传递待传的message参数即可
  logBoundDate("hello")
  logBoundDate("55555")
  log(date,"aasas")

}
