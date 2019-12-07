package com.gugu.scala.day02

object HightFunc extends App {
  // 高阶函数: 将其他函数作为参数或其结果是函数的函数
  // 定义一个方法, 参数为带一个整型参数返回值为整型的函数f 和 一个整型参数v, 返回值为一个函数
  def apply(f: Int => String, v: Int)= f(v)
  // 定义一个方法, 参数为一个整型参数, 返回值为String
  def layout(x:Int) = "["+x.toString+"]"
  // 调用
  println(apply(layout, 10))
}
