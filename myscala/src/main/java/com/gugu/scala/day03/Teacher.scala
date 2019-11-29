package com.gugu.scala.day03

private[gugu] class Teacher (var name:String,val age: Int) {
  var sex: String = _
  var prov: String = _

  def this(name: String, age: Int, sex: String) = {
    this(name, age)
    this.sex = sex
  }
  def this(name: String, age: Int, sex: String, prov: String) = {
    this(name,age,sex)
    this.prov = prov
  }

}
object Teacher{
  def main(args: Array[String]): Unit = {
    var hh = new Teacher("hh",15)
    println(hh.name)
  }
}