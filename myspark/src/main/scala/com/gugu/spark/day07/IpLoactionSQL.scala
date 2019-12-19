package com.gugu.spark.day07

import com.gugu.spark.day04.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IpLoactionSQL {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("IpLoactionSQL").master("local[*]").getOrCreate()
    //取到HDFS中的ip规则
    import sparkSession.implicits._
    val rulesLines: Dataset[String] = sparkSession.read.textFile(args(0))
    //整理ip规则数据()
    val ruleDataFrame: DataFrame = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")
    //创建RDD，读取访问日志
    val accessLines: Dataset[String] = sparkSession.read.textFile(args(1))
    //整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      //将log日志的每一行进行切分
      val fields: Array[String] = log.split("[|]")
      val ip: String = fields(1)
      //将ip转换成十进制
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")
    ruleDataFrame.createOrReplaceTempView("v_rules")
    ipDataFrame.createOrReplaceTempView("v_ips")
    val result: DataFrame = sparkSession.sql("select province, count(*) counts from v_ips join v_rules on (ip_num >= snum and ip_num <= enum)" +
      " group by province order by counts desc")
    result.show()
    sparkSession.stop()
  }
}
