package com.gugu.scala.day03

abstract class Animal {
  println("hhh constract")
  val name: String = "animal"
  def sleep()
  def eat(f: String) ={
    println("eat $f")
  }
}
