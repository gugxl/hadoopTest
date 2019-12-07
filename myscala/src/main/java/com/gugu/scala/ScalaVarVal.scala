package com.gugu.scala
class Student(val name: String, var age : Int) // 构造方法
object ScalaVarVal {
  def main(args: Array[String]): Unit = {
    // 变量的定义
    /**
     * 可以是var 和val修饰
     * var修饰的变量值可以更改
     * val修饰的变量值不可以改变，相当于java中final修饰的变量
     *
     * var | val 变量名称: 类型 = 值
     *
     * Unit 数据类型相当于java中void关键字，但是在scala它的标识形式是一对（）
     */
    val name = "aaaa"
    var age = 15
    print("name"+name + ", age"+age)
    val sql = s"select * from xx where name = ? and province = ? "
    println(f"姓名：$name%s  年龄：$age")
    printf("%s 学费 %4.2f, 网址是%s", name, 1234.146516, "xx")
    println(s"${name}")
    val i : Int = 1
    val s = if(i<10){
      i
      i* i
    }else{
      0
      100
    }
    var r: AnyVal = if(i < 15) i
    var r2 = if(i< 18) i else 18
    print(r2)
  }



}
