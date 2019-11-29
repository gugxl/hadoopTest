package com.gugu.scala.day02

object MyParame {
  def add(ints : Int*):Int ={
    var sum = 0
    for( i <- ints){
      sum += i
    }
    sum
  }
  def main(args: Array[String]): Unit = {
//    println(add(1,2))
    val arr:Array[String] = Array("hello gugu hello mimi", "hello liangliang hello")
    val wordsArray:Array[Array[String]] = arr.map(x => x.split(" "))
    val words:Array[String] = wordsArray.flatten
    val wordsMap:Map[String, Array[String]] = words.groupBy(x => x)
    val wordsCount:Map[String, Int] = wordsMap.mapValues(_.length)
    val list:List[(String, Int)] = wordsCount.toList
    val result:List[(String, Int)] = list.sortBy(x => -x._2)
    println(result)
//    println(arr.map(x => x.split(" ")).flatten.groupBy(x => x).mapValues(_.length).toList.sortBy(x=> -x._2))
  }

}
