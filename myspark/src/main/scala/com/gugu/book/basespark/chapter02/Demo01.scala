package com.gugu.book.basespark.chapter02


import java.io.{FileNotFoundException, FileReader, IOException, PrintWriter}
import scala.collection.immutable
import scala.util.control.Breaks.{break, breakable}

object Demo01 {
    
    def fileFun(): Unit = {
        val outFile = new PrintWriter("input.txt")
        outFile.println("gxl")
        outFile.print("hello ")
        outFile.close()
    }
    
    def listFun(): Unit = {
        val strList = List("hadoop", "spark", " flink")
        println(strList.head)
        println(strList.tail)
        
        val otherList = "gugu" :: strList
        print(otherList)
        
        val otherList2 = 1 :: 2 :: 3 :: Nil
        println(otherList2)
    }
    
    def vectorList(): Unit = {
        val vec1 = Vector(1, 2)
        println(vec1)
        val vec2 = 3 +: 4 +: vec1
        println(vec2)
        
        val vec3 = vec2 :+ 5
        println(vec3)
    }
    
    def rangeFun(): Unit = {
        var r = Range(1, 5)
        println(r)
        println(1 to 5)
        println(1.to(5))
        
        println(1 until (5))
        println(1 to 10 by 3)
    }
    
    def setFun(): Unit = {
        var mySet = Set("Hadoop", "Spark")
        mySet += "Scala"
        println(mySet)
        
        val mySet2 = scala.collection.mutable.Set("Database", "mysql")
        mySet2 += "oracle"
        println(mySet2)
    }
    
    def mapFun(): Unit = {
        var users = Map("gxl" -> 25, "小可爱" -> 22)
        println(users("gxl"))
        //        println(users("zz")) 直接获取不存在的key会异常
        println(users.contains("zz"))
        val age = if (users.contains("zz")) users.contains("zz") else 0
        println(age)
        
        users += ("xiaogu" -> 24)
        users += ("xiaogu" -> 25)
        println(users)
    }
    
    def iteratorFun(): Unit = {
        val iter = Iterator("hadoop", "hive")
        while (iter.hasNext)
            println(iter.next())
    }
    
    def main(args: Array[String]): Unit = {
        //        condition()
        //        circulate()
        //        exceptionFun()
        //        breakFun()
        //        arrayFun()
        //        println(3 max 8)
        //        val x = readLine()
        //        println(x)
        //        fileFun()
        //        listFun()
        //        vectorList()
        //        rangeFun()
        //        setFun()
        //        mapFun()
        iteratorFun()
    }
    
    def arrayFun(): Unit = {
        //        数组
        val intValue = new Array[Int](3)
        intValue.update(1, 2)
        intValue.update(0, 8)
        intValue(2) = 10
        for (i <- intValue) println(i)
        val stringArray: Array[String] = Array("gugu", "gxl")
        //        元组
        val tuple1 = ("bigData", 2023, 20)
        println(s"tuple1 = ${tuple1._1}")
        
        //        列表
        var strList: immutable.Seq[String] = List("Hadoop", "spark")
        
        
    }
    
    def breakFun(): Unit = {
        val array = Array(1, 5, 3, 9)
        // 跳出循环
        breakable {
            for (i <- array) {
                if (i > 4) break
                println(i)
            }
            
        }
        //        跳过某次执行
        for (i <- array) {
            breakable {
                if (i > 4) break
                println(i)
            }
        }
    }
    
    def exceptionFun(): Unit = {
        
        try {
            var f: FileReader = new FileReader("input.txt")
        } catch {
            case ex: FileNotFoundException => ex.printStackTrace()
            case ex: IOException => ex.printStackTrace()
        } finally {
            print("结束")
        }
    }
    
    def circulate(): Unit = {
        // 循环语句
        for (i <- 1 to 5) {
            println(i)
        }
        
        for (i <- Array(1, 2, 5)) {
            println(i)
        }
        
        for (i <- 1 to 5 if i % 2 == 0) println(i)
        
        
        var b: immutable.Seq[Int] = for (i <- 1 to 10 if i % 2 != 0) yield {
            print(i + " "); i
        }
        print(b)
    }
    
    def condition(): Unit = {
        //   条件语句
        var x = 6
        if (x > 3) {
            println("x > 0")
        } else if (x > 0) {
            println("x > 0")
        } else {
            println("x < 0")
        }
        // 对于scala if 语句是有返回值的
        var a: String = if (x > 3) "正数" else "复数"
        
        println(a)
    }
    
}
