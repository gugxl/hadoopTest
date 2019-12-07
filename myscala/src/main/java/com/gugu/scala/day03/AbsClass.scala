package com.gugu.scala.day03

abstract class AbsClass {
  def eat(food:String):String
  def swimming(style: String): Unit ={
    println(s"$style 这么游...")
  }
}
