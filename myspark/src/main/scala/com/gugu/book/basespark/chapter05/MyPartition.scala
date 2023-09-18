package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
//自定义分区类，需要继承org.apache.spark.Partitioner类
class MyPartition(numParts: Int) extends Partitioner {
    //覆盖分区数
    override def numPartitions: Int = numParts
    //覆盖分区号获取函数
    override def getPartition(key: Any): Int = {
        key.toString.hashCode() % 10
    }
    
}

object TestMyPartition {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("TestMyPartition").setMaster("local[*]")
        val context = new SparkContext(conf)
        val data: RDD[Int] = context.parallelize(1 to 10, 5)
        
        data.map((_, 1)).partitionBy(new MyPartition(10)).map(_._1).saveAsTextFile("/data/test/partition")
    }
}
