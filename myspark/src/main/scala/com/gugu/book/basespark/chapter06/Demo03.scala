package com.gugu.book.basespark.chapter06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Demo03 {
    
    def craeteDataSet(spark: SparkSession): Unit = {
        import spark.implicits._
        val path = "file:///D:\\applicationfiles\\data\\people.txt"
        
        val ds: Dataset[Int] = spark.createDataset(1 to 5)
        ds.show()
        
        val ds2: Dataset[String] = spark.createDataset(spark.sparkContext.textFile(path))
        ds2.show()
    }
    
    def craeteDataSet2(spark: SparkSession) = {
        import spark.implicits._
        val data = List(Person("ZhangSan", 23), Person("LiSi", 35))
        val ds: Dataset[Person] = data.toDS
        ds.show()
    }
    
    
    
    def craeteDataSetByDataFrame(spark: SparkSession) = {
        import spark.implicits._
        val path = "file:///D:\\applicationfiles\\data\\people.json"
        val peopleDF: DataFrame = spark.read.json(path)
        val person: Dataset[Person] = peopleDF.as[Person]
        person.show()
    }
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo01")
            .master("local").getOrCreate()
        //        craeteDataSet(spark)
//        craeteDataSet2(spark)
        craeteDataSetByDataFrame(spark)
        spark.stop()
    }
    
    case class Person(name: String, age: Long)
}
