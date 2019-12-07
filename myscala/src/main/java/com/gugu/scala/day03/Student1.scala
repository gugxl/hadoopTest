package com.gugu.scala.day03

object Student1 extends StudentTrait {
  type T = String
  def main(args: Array[String]): Unit = {
    Student1.learn("String")
  }
}
object Student2 extends StudentTrait{
  type T = Int

  def main(args: Array[String]): Unit = {
    Student2.learn(1000)
  }
}
