package com.gugu.scala.day01

object Hello {
  val name ="张三"
  def main(args: Array[String]): Unit = {

    var age = 25
//    println(f"姓名：$name","年龄：$age")
    val stu = new Student("小谷", 25)
    printf(s"${stu.name}")
    println()
    val i = 12

    val s = if(i>10) i else if(i == 8) 8 else 0
    println(s)

    println("${5+50}")
 }

  def apply(say: String)={
    print(s"你好：${say}")
  }

  case class Student(name: String, age: Int)

}
class Hello{

}
