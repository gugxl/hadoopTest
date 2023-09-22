package com.gugu.book.basespark.chapter07

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

object NetworkWordCountStatefulMySQL {
    def main(args: Array[String]): Unit = {
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.foldLeft(0)(_ + _)
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
        }
        
        val conf: SparkConf = new SparkConf().setAppName("NetworkWordCountStatefulMySQL").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(5))
        ssc.checkpoint("file:///D:\\applicationfiles\\data\\kafka\\checkpoint")
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val wordDstream: DStream[(String, Int)] = words.map((_, 1))
        val stateDstream: DStream[(String, Int)] = wordDstream.updateStateByKey(updateFunc)
        stateDstream.print()
        // 吧数据保存在mysql中
        stateDstream.foreachRDD(rdd => {
            def func(records: Iterator[(String, Int)]): Unit = {
                var connection: Connection = null
                var stmt: PreparedStatement = null
                val url = "jdbc:mysql://192.168.2.1:3306/mybatis_plus?useUnicode=true&characterEncoding=utf8"
                val user = "root"
                val password = "root"
                try {
                    connection = DriverManager.getConnection(url, user, password)
                    records.foreach(record => {
                        val sql = "insert into wordcount(word,count) values (?,?)"
                        stmt = connection.prepareStatement(sql)
                        stmt.setString(1, record._1.trim)
                        stmt.setInt(2, record._2.toInt)
                        stmt.executeUpdate()
                    })
                } catch {
                    case e: Exception => e.printStackTrace()
                } finally {
                    if (stmt != null) stmt.close()
                    if (connection != null) connection.close()
                }
            }
            
            val repartitionedRDD: RDD[(String, Int)] = rdd.repartition(3)
            repartitionedRDD.foreachPartition(func)
            
        })
        
        ssc.start()
        ssc.awaitTermination()
    }
    
}
