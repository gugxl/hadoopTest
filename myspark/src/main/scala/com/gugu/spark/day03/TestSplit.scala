package com.gugu.spark.day03

import java.net.URL

object TestSplit {
  def main(args: Array[String]): Unit = {
    val line = "http://bigdata.edu360.cn/laozhao"
//    val index: Int = line.lastIndexOf("/")
//    val teacher: String = line.substring(index+1)
//    val httpHost: String = line.substring(0,index)
//    val subject: String = new URL(httpHost).getHost.split("[.]")(0)
//    println(subject+","+teacher)
    print(new URL(line).getHost.split("[.]")(0))
  }

}
