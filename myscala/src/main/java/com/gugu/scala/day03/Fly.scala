package com.gugu.scala.day03

trait Fly {
  final val name = "bird"
  def fly(name : String)={
    println(s"看，$name 在飞...")
  }
}
