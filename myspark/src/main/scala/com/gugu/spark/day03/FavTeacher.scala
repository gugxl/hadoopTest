package com.gugu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FavTeacher").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据
    val lines: RDD[String] = sc.textFile(args(0))
    //整理数据
    val teacherAndOne: RDD[(String, Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      (teacher, 1)
    })

    //聚合
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //触发Action执行计算
    val result: Array[(String, Int)] = sorted.collect()
    //打印
    println(result.toBuffer)
    sc.stop()
  }
}
