package com.gugu.scala.day02

object ArrayOpt {
  def main(args: Array[String]): Unit = {
    val arr = Array[Int](1,3,5,7,9)
    // map 映射
    val fx = (x: Int) => x * 10
    // arr map映射操作之后会返回一个新的数组
    val r1: Array[Int] = arr.map(fx)
    arr.map((x:Int) => x * 10)
    arr.map(x => x * 10)
    arr.map(_ * 10)
    // flatten 扁平化操作
    val arr1: Array[String] = Array[String]("hello gugu hello tom", "htello huhuhu huhu dd")
    // Array(Array("hello","gugu","hello", "tom"), Array("htello", "huhuhu", "huhu", "dd"))
    val arr2: Array[Array[String]] = arr1.map(_.split(" "))
//    Array("hello","gugu","hello", "tom","htello", "huhuhu", "huhu", "dd") 扁平化操作
    arr2.flatten

    // flatMap = map -> flatten
//    arr1.flatMap(_.split(" ")).map(println)
    // 求每个单词出现的数量 word count
    //Array("hello","gugu","hello", "tom","hello", "hello", "hello", "tom")
    val r3 = arr1.flatMap(x => x.split(" "))
      // Map("hello"  -> Array("hello", hello, hello), tom -> Array("tom")
      .groupBy(x => x)
      //
      .mapValues(x => x.length).toList.sortBy(x => - x._2)
//    println(r3)
    val r4 = arr1.flatMap(_.split(" ")).groupBy(x => x).mapValues(_.length).toList.sortBy(y => - y._2)
    println(r4)
  }

}
