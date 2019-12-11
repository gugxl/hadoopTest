package com.gugu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSort5").setMaster("local[*]")
    //充分利用元组的比较规则，元组的比较规则：先比第一，相等再比第二个
    val sc = new SparkContext(conf)
    val users = Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    val lines: RDD[String] = sc.parallelize(users)
    val tpRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      val name: String = fields(0)
      val age: Int = fields(1).toInt
      val fv: Int = fields(2).toInt
      (name, age, fv)
    })
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => (-tp._3, tp._2))
    println(sorted.collect().toBuffer)
    sc.stop()
  }

}
