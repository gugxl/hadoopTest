package com.gugu.scala.day03

object ScalaSingleton {
  def saySomething(msg: String)={
    println(msg)
  }
}
object test{
  def main(args: Array[String]): Unit = {
    ScalaSingleton.saySomething("mmmm....")
    println(ScalaSingleton)
    println(ScalaSingleton)
  }
}