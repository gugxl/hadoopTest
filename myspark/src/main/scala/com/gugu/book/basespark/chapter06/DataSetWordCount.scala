package com.gugu.book.basespark.chapter06

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}

object DataSetWordCount {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("DataSetWordCount").getOrCreate()
        import spark.implicits._
        val data: Dataset[String] = spark.read.text("file:///D:\\applicationfiles\\data\\ml-25m\\README.txt").as[String]
        val words: Dataset[String] = data.flatMap(_.split(" "))
        val groupedWords: KeyValueGroupedDataset[String, String] = words.groupByKey(_.toLowerCase())
        val counts: Dataset[(String, Long)] = groupedWords.count()
        counts.show()
    }
    
}
