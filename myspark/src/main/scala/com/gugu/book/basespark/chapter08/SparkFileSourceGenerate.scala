package com.gugu.book.basespark.chapter08

import java.io.{File, PrintWriter}
import java.util.Date
import scala.util.Random

object SparkFileSourceGenerate {
    val TEST_DATA_TEMP_DIR = "D:\\applicationfiles\\data\\tmp\\"
    val TEST_DATA_DIR = "D:\\applicationfiles\\data\\test\\"
    
    val ACTION_DEF = List("login", "logout", "purchase")
    val DISTRICT_DEF = List("fujian", "beijing", "shanghai", "guangzhou")
    
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
    
    //测试环境的恢复，对文件夹进行清理
    def test_tearDown(): Unit = {
        val dir = new File(TEST_DATA_DIR)
        if (dir.exists()) {
            val files: Array[File] = dir.listFiles()
            for (file <- files) {
                file.delete()
            }
        }
        dir.delete()
    }
    
    def write_and_move(filename: String, data: String): Unit = {
        val file = new File(TEST_DATA_TEMP_DIR + filename)
        val writer = new PrintWriter(file)
        writer.write(data)
        writer.close()
        file.renameTo(new File(TEST_DATA_DIR + filename))
    }
    
    def main(args: Array[String]): Unit = {
        
        test_setUp()
        
        for (i <- 1 to 1000) {
            val filename = "e-mall-" + i + ".json"
            var content = ""
            for (j <- 1 to 100) {
                //内容是不超过100行的随机JSON行
                //格式为{"evenTime":1546939167,"action":"logout","district":"fujian"}\n
                val eventime = new Date().getTime.toString.substring(0, 10)
                val action_def = Random.shuffle(ACTION_DEF).head
                val district_def = Random.shuffle(DISTRICT_DEF).head
                content = content + "{\"eventTime\": " + eventime + ", \"action\": \"" + action_def + "\", \"district\": \"" + district_def + "\"}\n"
            }
            write_and_move(filename, content)
            Thread.sleep(1000)
        }
        test_tearDown()
    }
    
}
