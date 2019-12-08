package com.gugu.scala.day03

import java.util.Comparator

/**
 * 视图界定/上下文界定
 *  View bounds
 * <% 的意思是“view bounds” (视界)， 它比<:适用的范围更广， 除了所有的子类型， 还允许
 * 隐式转换过去的类型。
 */
class CmPare2[T <% Ordered[T]](first: T, second: T) {
  // 如果你觉得 compareTo 看着不爽， 就想使用 > 、 < 这种， 那么你可以将 Comparable[T] 改成 Ordered[T]
  def bigger = if(first > second) first else second
}
// char 类型 没有实现 Comparable 接口， 需要隐式的进行装箱操作 char -> Character
// 此时就不能使用 <: 上界， 的使用视图界定 <%
 private object Test4{
  def main(args: Array[String]): Unit = {
    implicit val cp =new Comparator[Int] {
      override def compare(o1: Int, o2: Int): Int = {
        o1 - o2
      }
    }
    // 求 2 个参数的最大值， 还有一个隐式参数 cp, 调用者在调用该方法时， 编译器会自动从上下文中找    这样的隐式参数
    def max[T](a: T, b:T)(implicit cp: Comparator[T]): T ={
      if (cp.compare(a,b) >0 ) a else b
    }

    def max2[T: Comparator](a:T, b:T): Unit ={
      def inner(implicit c: Comparator[T]) = c.compare(a,b)
      if(inner> 0) a else b
    }
    val bigger2: Char = new CmPare2('a','e').bigger
    println(bigger2)
  }
}