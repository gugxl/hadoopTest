package com.gugu.spark.day10

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

object IPUtils {
  def broadcastIpRules(ssc: StreamingContext,ipRulesPath: String) ={
    //现获取sparkContext
    val sc: SparkContext = ssc.sparkContext
    val rulesLines: RDD[String] = sc.textFile(ipRulesPath)
    //整理ip规则数据
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    })
    //将分散在多个Executor中的部分IP规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()
    //将Driver端的数据广播到Executor
    //广播变量的引用（还在Driver端）
    sc.broadcast(rulesInDriver)

  }
}
