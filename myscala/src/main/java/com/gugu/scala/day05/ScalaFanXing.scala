package com.gugu.scala.day05

import com.gugu.scala.day05.ClothesEnum.ClothesEnum


/**
 * 泛型： 就是类型约束
 *
 * List<T>
 */
abstract class Message[C](cotent : C)
class StrMessage(content:String) extends Message(content)
class IntMessage(content:Int) extends Message(content)
// 定义一个泛型类衣服
class Clothes[A,B,C](val clothType: A,val color: B,val clothesSize : C)
// 枚举类
object ClothesEnum extends Enumeration{
  type ClothesEnum = Value
  val 上衣, 内衣, 裤子 = Value
}
object ScalaFanXing {
  def main(args: Array[String]): Unit = {

    val cloth = new Clothes[ClothesEnum,String,Int](ClothesEnum.上衣, "black", 150)
    println(cloth.clothType)
  }



}
