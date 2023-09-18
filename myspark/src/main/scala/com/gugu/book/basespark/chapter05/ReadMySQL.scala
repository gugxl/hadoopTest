package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object ReadMySQL {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ReadMySQL").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val inputMySQL = new JdbcRDD(
            sc, () => {
                Class.forName("com.mysql.jdbc.Driver")
                DriverManager.getConnection("jdbc:mysql://192.168.2.1:3306/mybatis_plus?useUnicode=true&characterEncoding=utf8", "root", "root")
                
            },
            sql = "SELECT * FROM student where id >= ? and id <= ?",
            lowerBound = 1, upperBound = 2, numPartitions = 1,
            r => (r.getInt(1), r.getString(2), r.getString(3), r.getInt(4)))
        inputMySQL.foreach(println)
        sc.stop()
    }
    
}
