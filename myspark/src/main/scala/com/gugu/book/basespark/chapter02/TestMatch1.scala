package com.gugu.book.basespark.chapter02

import scala.io.StdIn

object TestMatch1 {
    def main(args: Array[String]): Unit = {
        val country = StdIn.readLine()
        //        字符串匹配
        country match {
            case "China" => println("中国")
            case "America" => println("美国")
            case "Japan" => println("日本")
            case _ => println("我不认识!")
        }
    }
    //    类型匹配
    for (elem <- List(6, 9, 0.618, "Spark", "Hadoop", 'Hello)) {
        val str = elem match {
            case i: Int => i + " is an int value." //匹配整型的值，并赋值给i
            case d: Double => d + " is a double value." //匹配浮点型的值
            case "Spark" => "Spark is found." //匹配特定的字符串
            case s: String => s + " is a string value." //匹配其它字符串
            case _ => "unexpected value：" + elem //与以上都不匹配
        }
        println(str)
    }
//    守卫式添加 过滤条件
    for (elem <- List(1, 2, 3, 4)) {
        elem match {
            case _ if (elem % 2 == 0) => println(elem + " is even.")
            case _ => println(elem + " is odd.")
        }
    }
    
}
