package com.gugu.scala.day03

import scala.util.Random

case class Message(msgContent: String){

}
/**
 * 默认实现了Serializable接口
 * 样例对象：case object 对象名
 * 模式匹配
 * 样例对象不能封装数据
 */
case object CheckHeatBeat
object TestCaseClass extends App{
//  val msg = Message("helo")
//  println(msg.msgContent)
  private val arr = Array("aaaa","bbbb","ccc")
  private val i: Int = Random.nextInt(arr.length)
  println(i)
  val name = arr(i)
  name match {
    case "aaaa" => println("4a")
    case "bbbb" => println("4b")
    case _ => println("nono")
  }
}
object TestCaseClass2 extends App {
  override def main(args: Array[String]): Unit = {
    var arr = Array("hello", 1, 2.0, TestCaseClass2, 2L)
    var el = arr(3)
    el match {
      case x: Int => println("1")
      case x: Long => println("L$x")
      case x: Boolean => println("L$x")
      case _ => println("no case")
    }
  }
}
