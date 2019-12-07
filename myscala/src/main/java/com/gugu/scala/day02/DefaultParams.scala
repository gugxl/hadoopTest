package com.gugu.scala.day02

object DefaultParams {
  def add(a : Int = 5, b: Int = 3) ={
    a + b
  }

  def main(args: Array[String]): Unit = {
    println(add())// 调用时，如果不传递参数，即会使用函数或者方法的默认值
    println(add(4,8))// 调用时，如果传递了参数值，则使用传递的参数值
    println(add(6))// 调用时，6覆盖了a=5
    println(add(b=5))
//    println(add(c=5,a=9))覆盖参数时，参数的名称必须和方法定义的名称保持一致
  }

}
