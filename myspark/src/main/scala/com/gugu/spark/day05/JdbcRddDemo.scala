package com.gugu.spark.day05

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRddDemo {
  val getConn = () =>{
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8","root","root")

  }
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //创建RDD，这个RDD会记录以后从MySQL中读数据
    //new 了RDD，里面没有真正要计算的数据，而是告诉这个RDD，以后触发Action时到哪里读取数据
    val jdbcRDD = new JdbcRDD(
      sc,
      getConn,
      "SELECT * FROM logs WHERE id >= ? AND id < ?",
      1,
      5,
      2,
      rs => {
        val id = rs.getInt(1)
        val name: String = rs.getString(2)
        val age: Int = rs.getInt(3)
        (id, name, age)
      }
    )
    val result: Array[(Int, String, Int)] = jdbcRDD.collect()
    println(result.toBuffer)
    sc.stop()
  }
}
