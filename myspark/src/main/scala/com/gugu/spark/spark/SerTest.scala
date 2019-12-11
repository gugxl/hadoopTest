package com.gugu.spark.spark

import java.net.InetAddress

import org.apache.commons.digester.Rules
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerTest {
  def main(args: Array[String]): Unit = {
    //在Driver端被实例化
//    val rules = Rules

    //println("@@@@@@@@@@@@" + rules.toString + "@@@@@@@@@@@@")
    val conf: SparkConf = new SparkConf().setAppName("SerTest")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val result: RDD[Unit] = lines.map(word => {
//      val rules = new Rules
      //函数的执行是在Executor执行的（Task中执行的）
      val hostName: () => String = InetAddress.getLocalHost.getHostName
      val threadName: () => String = Thread.currentThread().getName
      //Rules.rulesMap在哪一端被初始化的？
      (hostName, threadName)
    })
    result.saveAsTextFile(args(1))
    sc.stop()
  }
}
