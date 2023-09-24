package com.gugu.book.basespark.chapter08

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, PrintWriter}

object StructuredNetworkWordCountWindowedDelay {
    val TEST_DATA_DIR = "D:\\applicationfiles\\data\\tmp2\\"
    val TEST_DATA_DIR_SPARK = "file:///D:\\applicationfiles\\data\\test2\\"
    
    def test_setUp(): Unit = {
        val dir = new File(TEST_DATA_DIR)
        if (dir.exists()) {
            val files: Array[File] = dir.listFiles()
            for (file <- files) {
                file.delete()
            }
        }
        dir.delete()
        //创建目录
        dir.mkdir()
    }
    
    def del(file: File): Unit = {
        if (file.isDirectory) {
            
            val files = file.listFiles()
            for (f <- files) {
                del(f)
            }
        } else if (file.isFile()) {
            file.delete()
        }
    }
    
    def test_tearDown(): Unit = {
        val dir = new File(TEST_DATA_DIR)
        if (dir.exists()) {
            val files: Array[File] = dir.listFiles()
            for (file <- files) {
                del(file)
            }
        }
        dir.delete()
    }
    
    def write_to_csv(filename: String, data: String): Unit = {
        val file = new File(TEST_DATA_DIR + filename)
        val writer = new PrintWriter(file)
        writer.write(data)
        writer.close()
    }
    
    
    def main(args: Array[String]): Unit = {
        test_setUp()
        val spark: SparkSession = SparkSession.builder().appName("StructuredNetworkWordCountWindowedDelay")
            .master("local").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        val schema = new StructType()
            .add("word", StringType, true)
            .add("eventTime", TimestampType, true)
        
        val lines: DataFrame = spark.readStream
            .format("csv")
            .schema(schema)
            .option("sep", ";")
            .option("header", "false")
            .load(TEST_DATA_DIR_SPARK)
        
        val windowDuration = "1 hour"
        
        import spark.implicits._
        val windowCounts: DataFrame = lines
            .withWatermark("eventTime", windowDuration)
            .groupBy($"word", window($"eventTime", windowDuration))
            .count()
        
        val query: StreamingQuery = windowCounts
            .writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", false)
            .option("checkpointLocation", "file:///D:\\applicationfiles\\data\\ck6")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .start()
        
        write_to_csv("file1.csv",
            """
                    正常;2022-01-31 08:00:00
                    正常;2022-01-31 08:10:00
                    正常;2022-01-31 08:20:00
                    """)
        
        write_to_csv("file2.csv",
            """
                   正常;2022-01-31 20:00:00
                   一小时以内延迟到达;2022-01-31 10:00:00
                   一小时以内延迟到达;2022-01-31 10:50:00
                   """)
        
        query.processAllAvailable()
        write_to_csv("file3.csv",
            """
                   正常;2022-01-31 20:00:00
                   一小时外延迟到达;2022-01-31 10:00:00
                   一小时外延迟到达;2022-01-31 10:50:00
                   一小时以内延迟到达;2022-01-31 19:00:00
                   """)
        
        query.processAllAvailable()
        query.stop()
        test_tearDown()
        
        
    }
}
