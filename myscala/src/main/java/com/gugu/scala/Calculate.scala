package com.gugu.scala

object Calculate {
  // add方法拥有2个Int类型的参数， 返回值为2个Int的和
  def add(a: Int, b: Int) ={
    a+b
  }
  // add2方法拥有3个参数，第一个参数是一个函数， 第二个，第三个为Int类型的参数
  // 第一个参数：
  //     是拥有2个Int类型的参数，返回值为Int类型的函数
  def add2(f:(Int, Int) => Int, a: Int, b: Int) = {
    f(a, b)
  }

  def add3(a:Int => Int, b: Int): Int ={
    a(b)+b
  }

  val f1 = (x: Int) => x * 10
  def main(args: Array[String]): Unit = {
    val add1 = add(1, 2+2);
    println(add1)

    val r2 = add2((a:Int,b:Int) => a+b, 4 ,5)
    println(r2)
    val i: Int = add3(f1, 5)

    print(i)
  }

}
