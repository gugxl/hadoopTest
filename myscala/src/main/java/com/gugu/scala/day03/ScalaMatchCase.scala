package com.gugu.scala.day03
case class SendHeartBeat(id:String, time : Long)
case object CheckTimeOutWorker
/**
 * 模式匹配 match case
 * 一旦一个case 匹配上了，就不会再往下匹配了
 */
object ScalaMatchCase {
  def main(args: Array[String]): Unit = {
    // 匹配字符串内容
    def contentMatch(str: String) = str match{
        case "hello" => println("hello")
        case "Dog" => println("Dog")
        case "1" => println("1")
        case "1" => println("2")
        case _ => println("匹配不上") // _ 任意内容
    }

    println("----------匹配字符串内容--------")

    contentMatch("hello")
    contentMatch("Dog")
    contentMatch("1")
    contentMatch("aa")
    // 匹配数据类型
    println("----------匹配数据类型--------")
    def typeMatch(tp: Any) = tp match {
      case x: Int => println(s"Int $x")
      case y: String =>  println(s"String $y")
      case y: Long =>  println(s"Long $y")
      case y: Boolean =>  println(s"Boolean $y")
      case _ => println("匹配不上")
    }
    typeMatch(1)
    typeMatch(10L)
    typeMatch(true)
    typeMatch("Scala")

    // 匹配Array
    println("----------匹配Array--------")
    def arrayMatch(arr: Any) = arr match {
        case Array(0) => println("只有一个0元素的数组")
        case Array(0, _) => println("以0开头的，拥有2个元素的数组")
        case Array(1, _, 3) => println("已1开头，3结尾，中间为任意元素的三个元素的数组")
        case Array(8, _*) => println("已8开头，N个元素的数组") // _*标识0个或者多个任意类型的数据
    }
    arrayMatch(Array(0))
    arrayMatch(Array(0, "1"))
    arrayMatch(Array(1, true, 3))
    arrayMatch(Array(8,9,10,100,666))
    // 匹配List
    println("----------匹配List--------")
    def listMatch(list: Any) = list match {
      case 0 :: Nil => print("只有一个0元素的List")
      case 7 :: 9 :: Nil => println("只有7和9元素的List")
      case x:: y::z :: Nil => println("只有三个元素的List")
      case m :: n if n.length > 0 => println("------") // 拥有head，和 tail的数组   if n.length > 0  守卫
      case _ => println("匹配不上")
    }
    listMatch(List(0))
    listMatch(List(7,9))
    listMatch(List(8,9, 666))
    listMatch(List(666))

    // 匹配元组
    println("----------匹配元组--------")
    def tupleMatch(tuple: Any) = tuple match {
      case(0, _) => println("元组的第一个元素为0， 第二个元素为任意类型的数据，且只有2个元素")
      case (x,m,k) => println("拥有三个元素的元组")
      case (_,"AK47") => println("AK47")
    }
    tupleMatch((0,1))
    tupleMatch(("2",1,true))
    tupleMatch((ScalaMatchCase, "AK47"))

    // 匹配对象
    println("----------匹配对象--------")
    def objMatch(obj: Any) = obj match {
      case SendHeartBeat(x,y) => println(s"$x $y")
      case CheckTimeOutWorker => println("CheckTimeOutWorker")
      case "registerWorker" => println("registerWorker")
    }

    objMatch(SendHeartBeat("appid0000001", System.currentTimeMillis()))
    objMatch(SendHeartBeat("aaaa",100L))
    objMatch(CheckTimeOutWorker)
    objMatch("registerWorker")

  }
}
