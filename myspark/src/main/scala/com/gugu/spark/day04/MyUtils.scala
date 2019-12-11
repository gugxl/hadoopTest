package com.gugu.spark.day04

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

object MyUtils {
  def ip2Long(ip: String)={
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for ( i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
  def readRules(path:String): Array[(Long, Long, String)] ={
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }
  def binarySearch(lines: Array[(Long, Long, String)], ip:Long): Int = {
    var low = 0
    var high = lines.length - 1
    while(low <= high){
      val middle = (low + high)/2
      if((ip >= lines(middle)._1) && (ip <= lines(middle)._2)){
        return middle
      }else if(ip <= lines(middle)._1){
        high = middle - 1
      }else{
        low = middle + 1
      }
    }
    -1
  }
  def data2MySQL(it:Iterator[(String,Int)]): Unit ={
    //一个迭代器代表一个分区，分区中有多条数据
    //先获得一个JDBC连接
    val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8","root","root")
    //将数据通过Connection写入到数据库
    val statement: PreparedStatement = connection.prepareStatement("INSERT INTO access_long VALUES (?, ?)")
    //将分区中的数据一条一条写入到MySQL中
    it.foreach(tp =>{
      statement.setString(1, tp._1)
      statement.setInt(2, tp._2)
      statement.execute()
    })
    //将分区中的数据全部写完之后，在关闭连接
    if(statement != null)
      statement.close()
    if(connection != null)
      connection.close()
  }

  def main(args: Array[String]): Unit = {
    //数据是在内存中
    val rules: Array[(Long, Long, String)] = readRules("D:\\logs\\ip\\ip.txt")
    //将ip地址转换成十进制
    val ipNum: Long = ip2Long("114.215.43.42")
    val index: Int = binarySearch(rules, ipNum)
    val tp: (Long, Long, String) = rules(index)
    val province: String = tp._3
    println(province)
  }
}
