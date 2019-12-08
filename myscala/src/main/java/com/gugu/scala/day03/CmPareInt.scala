package com.gugu.scala.day03

class CmPareInt(first: Int,second: Int) {
  def bigger = if(first > second) first else second
}
class CmPareString(first: String,second: String){
  def bigger = if(first > second) first else second
}
// 如果还要定义 Double 类型的比较， 也许还需要比较 2 个类的比较， 咋办， 还重复的劳动吗？
// 其实我们可以使用泛型, 但是泛型类型必须实现了 Comparable, 相当于约束了泛型的范围
// T <: Comparable[T] 表示 T 类型是 Comparable 的实现类
// <: 在 Scala 中叫上界 upper bounds,类似 Java CmPare<T extends Comparable<T>>
// T >: xxx[T] 表示 T 类型是 xxx 的超类
// >: 在 Scala 中叫下界 lower bounds， 类似 Java CmPare<T super xxx<T>>
class CmPare[T <:Comparable[T] ](first: T, second: T){
  def bigger = if (first.compareTo(second) > 0) first else second

}
object Test3 {
  def main(args: Array[String]): Unit = {
    val cpi: Int = new CmPareInt(4,6).bigger
    val cps: String = new CmPareString("aaa","hello").bigger
    // 这样就更通用了， 这里必须使用装箱类型， 因为 Int 没有实现 Comparable 接口
    val cpiv: Integer = new CmPare(Integer.valueOf(5), Integer.valueOf(10)).bigger
    val cpsv: String = new CmPare("hadoop","hello").bigger
    println(s"cpi${cpi}")
    println(s"cps${cps}")
    println(s"cpiv${cpiv}")
    println(s"cpsv${cpsv}")
  }

}