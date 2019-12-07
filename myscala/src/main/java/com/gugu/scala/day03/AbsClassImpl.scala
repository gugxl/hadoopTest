package com.gugu.scala.day03

object AbsClassImpl extends AbsClass with Fly {
  override def eat(food: String): String = {
    s"$food 炒着吃"
  }

  def main(args: Array[String]): Unit = {
    AbsClassImpl.swimming("漂着")
  }
}
