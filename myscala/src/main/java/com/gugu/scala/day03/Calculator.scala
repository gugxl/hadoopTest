package com.gugu.scala.day03


object Test2{
 implicit class Calculator1(x: Int) {
    def add_10(a: Int) = (x+a)*10
  }

  def main(args: Array[String]): Unit = {
    val z = 2
    println(z.add_10(4))
  }
}

