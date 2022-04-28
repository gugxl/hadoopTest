package com.gugu.spark.day03

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher3").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val topN: Int = args(1).toInt
    val lines: RDD[String] = sc.textFile(args(0))
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val hostHot: String = line.substring(0, index)
      val subject: String = new URL(hostHot).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(_+_)
    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    //自定义一个分区器，并且按照指定的分区器进行分区
    val subjectParitioner = new SubjectParitioner(subjects)
    //partitionBy按照指定的分区规则进行分区
    //调用partitionBy时RDD的Key是(String, String)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(subjectParitioner)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })
    //将迭代器转换成list，然后排序，在转换成迭代器返回
    val result: Array[((String, String), Int)] = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
  //自定义分区器
  class SubjectParitioner(sbs : Array[String]) extends Partitioner{
    //相当于主构造器（new的时候回执行一次）
    //用于存放规则的一个map
    private val rules = new mutable.HashMap[String,Int]()
    var i = 0
    for(sb <- sbs){
      rules.put(sb, i)
      i+=1
    }
    //返回分区的数量（下一个RDD有多少分区）
    override def numPartitions: Int = sbs.length
    //根据传入的key计算分区标号
    //key是一个元组（String， String）
    override def getPartition(key: Any): Int = {
      //获取学科名称
      val subject: String = key.asInstanceOf[(String,String)]._1
      rules(subject)
    }
  }
}
