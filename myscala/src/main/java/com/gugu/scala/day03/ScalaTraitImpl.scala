package com.gugu.scala.day03

object ScalaTraitImpl extends ScalaTrait with Fly {
//  name = ""

  override def hello(name: String) ={
    println(s"hello $name")
  }

  override def small(name: String): Unit = {
    println(s"丁丁 对 $name 哈哈大笑")
  }

  def main(args: Array[String]): Unit = {
    ScalaTraitImpl.hello("hanmeimei")
    ScalaTraitImpl.small("老段")
    ScalaTraitImpl.fly("老羊")
  }
}
