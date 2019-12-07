package com.gugu.scala.day03
/**
 * 特质（interface）
 *
 * 在Scala中特质中可以定义有实现的方法，也可以定义没有实现的方法
 *
 */
trait ScalaTrait {
  /**
   * 没有任何实现的方法
   * @param name
   */
  def hello(name: String) ={
    println(name)
  }
  def small(name: String) ={
    println(s"ll 对${name}妩媚一笑")
  }
}
