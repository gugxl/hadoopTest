package com.gugu.scala.day03

class Dog extends Animal {
  override def sleep(): Unit = {
    println("趴着岁")
  }
  override def eat(f: String)={
    println("狗狗 eat $f")
  }


}

object Dog{
  def main(args: Array[String]): Unit = {
    type S = String
    var s: S = "小秘密"
    println(s)
  }
}
