package com.gugu.book.basespark.chapter05

import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

object WriteMySQL {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("WriteMySQL").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val rddData = sc.parallelize(List((3, "Rongcheng", "M", 26), (4, "Guanhua", "M", 27)))
        rddData.foreachPartition((iter) => {
            val connection: Connection = DriverManager.getConnection("jdbc:mysql://192.168.2.1:3306/mybatis_plus?useUnicode=true&characterEncoding=utf8", "root", "root")
            connection.setAutoCommit(false)
            val statement: PreparedStatement = connection.prepareStatement("INSERT INTO student(id,name,gender,age) VALUES (?,?,?,?)")
            
            iter.foreach(t => {
                statement.setInt(1, t._1)
                statement.setString(2, t._2)
                statement.setString(3, t._3)
                statement.setInt(4, t._4)
                statement.addBatch()
            })
            statement.executeBatch()
            connection.commit()
            connection.close()
        })
        sc.stop()
    }
    
}
