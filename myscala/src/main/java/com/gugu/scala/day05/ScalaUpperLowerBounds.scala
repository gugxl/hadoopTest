package com.gugu.scala.day05

class CmpInt(a:Int, b:Int) {
  def bigger = if(a>b) a else b
}
class cmpString(a: String, b:String){
  def bigger = if(a>b) a else b
}
/**
 * <: 上界 upper bounds
 * 类似java中的 <T extends Comparable>
 *     不会发生隐式转换，除非用户显示的指定
 *     T 实现了 Comparable 接口
 */
//class CmpComm[T <: Comparable[T]](o1 :T ,o2: T){
//  def bigger = if(o1.compareTo(o2)> 0) o1 else o2
//}
/**
 * <% 视图界定 view bounds
 *   会发生隐式转换
 */
//class CmpComm[T <% Comparable[T]](o1:T, o2:T){
//  def bigger = if(o1.compareTo(o2)>0) o1 else o2
//}
// 第二个版本
//class CmpComm[T <% Ordered[T]](o1:T, o2:T){
//  def bigger = if(o1 > o2) o1 else o2
//}
// Comparator 传递比较器
/**
 * 上下文界定
 *     也会隐式转换
 */
//class CmpComm[T: Ordered](o1:T,o2:T)(implicit cmptor:Ordering[T]){
//  def bigger = if(cmptor.compare(o1,o2) >0 ) o1 else o2
//}
//class CmpComm[T:Ordering](o1:T, o2:T){
//  def bigger = {
//    def inner(implicit cmptor: Ordering[T]) = cmptor.compare(o1,o2)
//    if(inner > 0) o1 else o2
//  }
//}
class CmpComm[T: Ordering](o1:T, o2:T){
  def bigger ={
    val cmptor = implicitly[Ordering[T]]
    if(cmptor.compare(o1,o2)>0) o1 else o2
  }
}
//class Students(val name:String, val age:Int) extends Ordered[Students]{
//  override def compare(that: Students): Int = {
//    this.age - that.age
//  }
//  override def toString: String = this.name+"\t"+this.age
//}
class Students(val name: String, val age :Int){
  override def toString: String = this.name+"\t"+this.age
}
object ScalaUpperLowerBounds{
  def main(args: Array[String]): Unit = {
//    val cmpInt = new CmpInt(5,9)
//    println(cmpInt.bigger)
//    val cmpcom = new CmpComm[Int](5,7) // 上界的时候会报错
//    println(cmpcom.bigger)
//    val cmpcom = new CmpComm(Integer.valueOf(2), Integer.valueOf(7))
//    val cmpcom = new CmpComm[Integer](1,2)
//    println(cmpcom.bigger)
    import MyImpicits._
    val tom = new Students("Tom", 18)
    val jim = new Students("jim", 23)
    val cmpcom = new CmpComm[Students](tom,jim)
    println(cmpcom.bigger.name)

  }
}