package com.gugu.scala.day03

class Student {
  // _ 表示一个占位符, 编译器会根据你变量的具体类型赋予相应初始值
  // 注意: 使用_ 占位符是, 变量类型必须指定
  var name : String = _
  // 错误代码, val 修饰的变量不能使用占位符
  // val age: Int = _
  val age : Int = 25
}
object Test1{
  val name:String ="zhangsan"
}