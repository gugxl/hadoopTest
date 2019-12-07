package com.gugu.scala.day03

object ParallelSeq {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4)
    println(list.par.fold(0)(_+_))
    println(list.par.foldLeft(0)(_+_))
//    println(list.par.aggregate(0))
  }

}
