package com.gugu.scala.day03

/**
 * 在Scala中定义类的用class关键字修饰
 * 这个类默认有一个空参构造器
 *
 * 定义在类名称后面的构造器叫主构造器
 *
 * 类的主构造器中的属性会定义成类的成员变量
 *
 * 如果主构造器中成员属性没有val|var修饰的话，该属性不能被访问,相当于对外没有提供get方法
 * 如果成员属性使用var修饰的话，相当于对外提供了get 和set 方法
 * 如果成员属性使用val修饰的话，相当于对外提供了 get
 *
 * 类的成员属性访问权限：
 *     如果类的主构造器中成员属性是private修饰的，它的set 和 get方法都是私有的，外部不能访问
 *
 *
 * 类的构造器访问权限
 *     在构造器前加修饰权限
 *     private 在主构造器之前，这说明该类的主构造器是私有的，外部类或者外部对象不能访问
 *     也适用于辅助构造器
 *
 *
 * 类的访问权限
 *     类的前面加上private[this] 标识这个类在当前包下都可见，当前包下的子包不可见
 *     类的前面加上private[包名] 表示这个类在当前包及其子包下都可见
 */

private[gugu] class Teacher (var name:String,val age: Int) {
  var sex: String = _
  var prov: String = _

  // 定义个辅助构造器，def this()
  def this(name: String, age: Int, sex: String) = {
    // 在辅助构造器中必须先调用主构造器
    this(name, age)
    this.sex = sex
  }
  def this(name: String, age: Int, sex: String, prov: String) = {
    this(name,age,sex)
    this.prov = prov
  }

}
// object Teacher 叫类的伴生对象
/**
 * 在伴生对象中可以访问类的私有成员方法和属性
 */
object Teacher{
  def main(args: Array[String]): Unit = {
    var hh = new Teacher("hh",15)
    println(hh.name)
  }
}