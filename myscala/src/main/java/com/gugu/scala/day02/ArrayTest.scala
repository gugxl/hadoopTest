package com.gugu.scala.day02

object ArrayTest {

  /**
   * 可变参数方法
   * def method1（str: String, a: Any*）
   *
   * 默认值参数方法
   * def method2(a: Int = 6, x: String = "a")
   * method2()
   * method2(x=“b”, a=8)
   *
   * 高阶函数：参数是函数 或者 返回值是函数
   * def method3(f: Int => Int, c: Int) = f(c)
   *
   *
   * 柯里化
   * def method4(a: Int)(b: Int, c: Int)
   * method(0)(4, 5)
   *
   * 偏函数：PartialFunction[参数类型，函数返回值类型]
   * def method5 :PartialFunction[Any, String] = {
   * case x: Int => x * 10
   * }
   *
   * 数组的定义
   * Array: 长度不可变，内容可变
   *
   * val arr = Array(1,4,6) => ??? => Array(10, 40 ,60)
   * 10 => function: 10 * 10 => 100
   * map => 映射  将集合进行某种操作，并且返回一个新的集合
   *     arr.map((x: Int) => x * 10)
   *     arr.map(x=> x * 10)
   *     arr.map(_ * 10)
   *
   * flatten 扁平化操作
   * Array[Array[String]] = Array(Array("1", "2"), Array("4", "5"))
   * 扁平化操作后：
   * Array[String] = Array("1", "2", "4", "5")
   *
   * flatMap
   * map + flatten
   *
   * foreach 对集合进行循环操作
   *     arr.foreach(f: Int => Unit)
   *
   *
   * WordCount
   *
   */

  def main(args: Array[String]): Unit = {
    val list = List(3,5,1)
    val i: Int = list.fold(1)(_+_)
    println(i)


  }
}
