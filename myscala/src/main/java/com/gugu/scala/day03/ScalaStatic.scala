package com.gugu.scala.day03
/**
 * 在scala中的object 是一个单例对象，没办法new
 * object中定义的成员变量 和 方法都是静态的
 *
 * 可以通过对象名.方法 或者.成员变量
 */
object ScalaStatic {
  val name ="zhangshan"
  var age = 25
  def saySomething(msg: String)={
    println(msg)
  }
  def apply(food: String)={
    println(s"米饭1碗 $food")
  }
}
