package com.gugu.book.basespark.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import java.util
import java.util.Properties

// DataFrame 的DSL语法
object Demo02 {
    
    def dfFun(spark: SparkSession): Unit = {
        val path = "file:///D:\\applicationfiles\\data\\people.json"
        val df: DataFrame = spark.read.json(path)
        df.printSchema()
        df.show()
        df.select(df("name"), df("age") + 1).show()
        
        df.filter(df("age") > 20).show()
        df.groupBy("age").count().show()
        
        df.sort(df("age").desc).show()
        df.sort(df("age").desc, df("name").asc).show()
        
        val df2: DataFrame = df.withColumn("IfWithAge", expr("CASE WHEN age is null THEN 'NO' ELSE 'YES' END"))
        df2.show()
        
        val df3: DataFrame = df2.drop("IfWithAge")
        df3.show()
        
        df.select(functions.max("age"), functions.avg("age"), functions.min("age")).show()
        
    }
    
    def dfSQLOperate(spark: SparkSession): Unit = {
        val df: DataFrame = spark.read.json("file:///D:\\applicationfiles\\data\\people.json")
        df.createTempView("people")
        spark.sql("SELECT * FROM people").show()
        spark.sql("SELECT name FROM people where age > 20").show()
        
    }
    
    def dfSQLFun(spark: SparkSession): Unit = {
        
        val schema: StructType = StructType(List(
            StructField("name", StringType, true),
            StructField("age", IntegerType, true),
            StructField("create_time", LongType, true)))
        val rows = new util.ArrayList[Row]()
        rows.add(Row("Xiaomei", 21, System.currentTimeMillis() / 1000))
        rows.add(Row("Xiaoming", 22, System.currentTimeMillis() / 1000))
        rows.add(Row("Xiaoxue", 23, System.currentTimeMillis() / 1000))
        val df: DataFrame = spark.createDataFrame(rows, schema)
        
        df.show()
        df.createTempView("user_info")
        spark.sql("SELECT name,age,from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss') FROM user_info").show()
        
    }
    
    def dfReflex(spark: SparkSession): Unit = {
        //        利用反射方式把RDD转换成DataFrame
        import spark.implicits._ //注意一定要导入包，支持把一个RDD隐式转换为一个DataFrame
        
        val path = "file:///D:\\applicationfiles\\data\\people.txt"
        val peopleDF: DataFrame = spark.sparkContext.textFile(path)
            .map(_.split(","))
            .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
            .toDF()
        peopleDF.createTempView("people")
        val personsRDD: DataFrame = spark.sql("select name,age from people where age > 20")
        //        truncate = false 防止列过长
        personsRDD.map(t => "Name: " + t(0) + "," + "Age: " + t(1)).show(truncate = false)
    }
    
    def dfProgram(spark: SparkSession): Unit = {
        val path = "file:///D:\\applicationfiles\\data\\people.txt"
        val fields = Array(
            StructField("name", StringType, true),
            StructField("age", IntegerType, true))
        import spark.implicits._ //注意一定要导入包，支持把一个RDD隐式转换为一个DataFrame
        
        //        表头
        val schema = StructType(fields)
        val peopleRDD: RDD[String] = spark.sparkContext.textFile(path)
        //        行数据
        val rowRDD: RDD[Row] = peopleRDD.map(_.split(","))
            .map(attributes => Row(attributes(0), attributes(1).trim.toInt))
        //        把表头和数据拼接起来
        val peopleDF: DataFrame = spark.createDataFrame(rowRDD, schema)
        //        注册临时表
        peopleDF.createTempView("people")
        
        val results: DataFrame = spark.sql("SELECT name,age FROM people")
        results.map(attributes => "name: " + attributes(0) + "," + "age:" + attributes(1)).show(truncate = false)
        
    }
    
    def dfReadMySQL(spark: SparkSession): Unit = {
        val jdbcDF: DataFrame = spark.read.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/mybatis_plus?characterEncoding=UTF-8")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("dbtable", "student")
            .option("user", "root")
            .option("password", "root")
            .load()
        
        jdbcDF.show()
        
    }
    
    def dfWriteMySQL(spark: SparkSession): Unit = {
        val studentRDD: RDD[Array[String]] = spark.sparkContext.parallelize(Array("5 Rongcheng M 26", "6 Guanhua M 27")).map(_.split(" "))
        val schema: StructType = StructType(List(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true),
            StructField("gender", StringType, true),
            StructField("age", IntegerType, true)))
        
        val rowRDD: RDD[Row] = studentRDD.map(p => Row(p(0).toInt, p(1), p(2), p(3).toInt))
        
        val studentDF: DataFrame = spark.createDataFrame(rowRDD, schema)
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "root")
        prop.put("driver", "com.mysql.jdbc.Driver")
        
        studentDF.write.mode("append")
            .jdbc("jdbc:mysql://localhost:3306/mybatis_plus?characterEncoding=UTF-8", "student", prop)
    }
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo01").master("local").getOrCreate()
        //        dfFun(spark)
        //        dfSQLOperate(spark)
        //        dfSQLFun(spark)
        //        dfReflex(spark)
        //        dfProgram(spark)
        //        dfReadMySQL(spark)
        dfWriteMySQL(spark)
        spark.stop()
    }
    
    
}

case class Person(name: String, age: Long)