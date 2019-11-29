package com.gugu.scala.day03

object ParalleSeq {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5,6)
    val i: Int = list.par.fold(100)(_+_)
    val i1: Int = list.par.foldLeft(100)(_+_)
    println(i)
    println(i1)
  }

}
