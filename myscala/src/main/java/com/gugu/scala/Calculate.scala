package com.gugu.scala

object Calculate {

  def add(a: Int, b: Int) ={
    a+b
  }

  def add2(f:(Int, Int) => Int, a: Int, b: Int) = {
    f(a, b)
  }

  def main(args: Array[String]): Unit = {
    val add1 = add(1, 2+2);
    println(add1)

    val r2 = add2((a:Int,b:Int) => a+b, 4 ,5)
    println(r2)
  }

}
