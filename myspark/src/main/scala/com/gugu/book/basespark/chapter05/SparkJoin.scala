package com.gugu.book.basespark.chapter05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkJoin {
    def main(args: Array[String]): Unit = {
        val regex = """^\d+$""".r
        val conf: SparkConf = new SparkConf().setAppName("SparkJoin").setMaster("local")
        val sc = new SparkContext(conf)
        val textFile: RDD[String] = sc.textFile("file:///D:\\applicationfiles\\data\\ml-25m\\ratings.csv")
        //extract (movieid, rating)
        val rating: RDD[(Int, Double)] = textFile.mapPartitionsWithIndex {
            (idx, iter) => if (idx == 0) iter.drop(1) else iter
        }
            .map(line => {
                val fileds: Array[String] = line.split(",")
                (fileds(1).toInt, fileds(2).toDouble)
            })
        // get (movieid,ave_rating)
        val movieScores: RDD[(Int, Double)] = rating.groupByKey().map(data => {
            val avg = data._2.sum / data._2.size
            (data._1, avg)
        })
        
        var movies: RDD[String] = sc.textFile("file:///D:\\applicationfiles\\data\\ml-25m\\movies.csv")
        val movieskey: RDD[(Int, (Int, String))] = movies.mapPartitionsWithIndex {
            (idx, iter) => if (idx == 0) iter.drop(1) else iter
        }.map(line => {
            val fileds: Array[String] = line.split(",")
            (fileds(0).toInt, fileds(1))
        }).keyBy(tup => tup._1)
        // <movie, averageRating, movieName>
        val result: RDD[(Int, Double, String)] = movieScores
            .keyBy(tup => tup._1)
            .join(movieskey)
            .filter(f => f._2._1._2 > 4.0)
            .map(f => (f._1, f._2._1._2, f._2._2._2))
        
        result.collect().foreach(r => println(r._1 + "\t" + r._2 + "\t" + r._3))
    }
    
}
