package com.gugu.scala.day03

object TestApp extends App {

  override def main(args: Array[String]): Unit = {
     var teacher: Teacher = new Teacher("张三",15)
    print(teacher.name)
    teacher.name = "历史"
    print(teacher.age)
  }
}
