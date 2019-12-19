package com.gugu.spark.day07

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("JdbcDataSource").master("local[*]").getOrCreate()
    //load这个方法会读取真正mysql的数据吗
    import sparkSession.implicits._
    val persons: DataFrame = sparkSession.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://master:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "t_person",
        "user" -> "root",
        "password" -> "root"
      )
    ).load()
//    persons.printSchema()
//    persons.show()
//    val filtered: Dataset[Row] = persons.filter(person => {
//      person.getAs[Int]("age") > 18
//    })
//    filtered.show()
//    //lambda表达式
//    val filtered2: Dataset[Row] = persons.filter($"age"<18)
//    filtered2.show()
//
    val filtered3: Dataset[Row] = persons.where($"age"<18)
    val result: DataFrame = filtered3.select($"id",$"name",$"age"*10).toDF("id","name","age")
    val props = new Properties()
    props.put("user","root")
    props.put("password","root")
//    result.write.mode("ignore").jdbc("jdbc:mysql://master:3306/bigdata","logs",props)
    //DataFrame保存成text时出错(只能保存一列)
//    result.write.text("D:\\logs\\text")

//    result.write.json("D:\\logs\\json2")
//    result.write.csv("D:\\logs\\csv2")
    result.write.parquet("hdfs://master:9000/test/parquet")
    result.show()
    sparkSession.stop()
  }
}
