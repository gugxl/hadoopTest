package com.gugu.day03

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GroupFavTeacher4 {
  def main(args: Array[String]): Unit = {
    val topN: Int = args(1).toInt
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val hostHot: String = line.substring(0, index)
      val subject: String = new URL(hostHot).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    //计算有多少学科
    val subjects: Array[String] = subjectTeacherAndOne.map(_._1._1).distinct().collect()
    //自定义一个分区器，并且按照指定的分区器进行分区
    val subjectParitioner = new SubjectParitioner(subjects)

    //聚合，聚合是就按照指定的分区器进行分区
    //该RDD一个分区内仅有一个学科的数据
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(subjectParitioner,_+_)
    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val result: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })
    result.saveAsTextFile(args(2))
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
