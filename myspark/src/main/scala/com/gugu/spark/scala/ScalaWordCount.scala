package com.gugu.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[*]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    var func = (index : Int, it :Iterator[Int]) => {
      it.map(e => s"part: $index, ele： $e \n")
    }

    val rdd2 = rdd1.mapPartitionsWithIndex(func)
    rdd2.collect()

    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
//    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))
    val line: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = line.flatMap(_.split(" "))
    //将单词和一组合
    val wordAndCount: RDD[(String, Int)] = words.map((_,1))
    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndCount.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}
