package com.gugu.scala

object CallbyName extends App{
  def currentTime(): Long ={
    println("打印系统当前时间,单位纳秒")
    System.nanoTime()
  }
  def delayed(f: => Long):Unit={
    print("delayed----------------")
    print("time ="+ f)
  }
  def delayed1(time: Long) ={
    print("delayed1-----------------")
    print("time1="+time)
  }
  // 调用方式一
  delayed(currentTime())
  // 调用方式二
  val time = currentTime()
  delayed1(time)
}
