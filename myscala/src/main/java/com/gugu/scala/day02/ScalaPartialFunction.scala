package com.gugu.scala.day02

object ScalaPartialFunction {
  def func(str: String)={
    if(str.equals("a")) 97
    0
  }
  /**
   * 偏函数：PartialFunction[参数类型，返回值类型]
   */
  def func2:PartialFunction[String, Int] ={
    case "a" => 97
    case _ => 0
  }
  def func3:PartialFunction[Any , Int]={
    case i: Int => i*10
  }

  def main(args: Array[String]): Unit = {
    println(func("a"))
    println(func2("a"))
    var arr = Array[Any](12, 3, 5, "hello")
    var arr2 = Array[Int](122, 5)
    val collect: Array[Int] = arr.collect(func3)
    arr2.map { case x: Int => x * 10 }
    print(collect.toBuffer)
  }
}
