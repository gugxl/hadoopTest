package com.gugu.spark.day03

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    val topN: Int = args(1).toInt
    val subjects = Array("bigdata", "javaee", "php")
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val hostHot: String = line.substring(0, index)
      val subject: String = new URL(hostHot).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)
    //cache到内存
//    val cached: RDD[((String, String), Int)] = reduced.cache()
    //scala的集合排序是在内存中进行的，但是内存有可能不够用
    //可以调用RDD的sortby方法，内存+磁盘进行排序
    for (sb <- subjects){
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2,false).take(topN)
      println(favTeacher.toBuffer)
    }
    sc.stop()
  }
}
