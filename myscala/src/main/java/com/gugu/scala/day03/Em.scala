package com.gugu.scala.day03

import com.gugu.scala.day03.Em.Em

object Em extends Enumeration {
  type Em = Value
  val 上衣, 内衣, 裤子, 袜子 = Value
}
abstract class Message1[T](s: T){
  def get:T=s
}
// 子类扩展的时候， 约定了具体的类
class StrMessage[String](msg: String) extends Message1(msg)
class IntMessage[Int](msg: Int) extends Message1(msg)
// 定义一个泛型类
class Clothes[A,B,C](val clothesType: A, var color : B, var size:C)
object Test5{
  def main(args: Array[String]): Unit = {
    val s = new StrMessage("i hate you !")
    val i = new IntMessage(25)
    println(s.get)
    println(i.get)
    // 创建一个对象， 传入的参数为 Em, String, Int
    val c1 = new Clothes(Em.上衣, "Red", 25)
    c1.size = 38
    println(c1.clothesType,c1.size)
    // new 的时候， 指定类型。 那么传入的参数， 必须是指定的类型
    val c2 = new Clothes[Em,String,String](Em.内衣, "黑色","C")
    println(c2.size)
    // 定义一个函数， 可以获取各类 List 的中间位置的值
    val list1 = List("a","b","c")
    val list2 = List(1,2,3,4,5,6)

    def getData[T](l : List[T]): Unit ={
      l(l.length/2)
    }

    println(getData(list1))
    println(getData(list2))
  }
}