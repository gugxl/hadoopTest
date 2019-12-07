package com.gugu.scala.day02

object ManyParams {
  /**
   * 可变参数， 在参数类型后面加上一个通配符 *
   */
  def add(ints: Int*)={
    var sum = 0
    for(int <- ints)
      sum+=int
    sum
  }
  /**
   * 可变参数一般放在参数列表的末尾
   */
  def add1(initValue: Int, ints: Int*)={
    var sum = initValue
    for(i <- ints){
      sum += i
    }
    sum
  }
  /**
   * Any 任意类型
   */
  def makePerson(params: Any * )={

  }

  def main(args: Array[String]): Unit = {
    println(add(1))
    println(add(1,2,3,5,6))
    println(add(1,2,36))
  }
}
