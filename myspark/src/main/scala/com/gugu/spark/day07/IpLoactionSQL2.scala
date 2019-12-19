package com.gugu.spark.day07

import com.gugu.spark.day04.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//jon的代价太昂贵，而且非常慢，解决思路是将表缓存起来（广播变量）
object IpLoactionSQL2 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("IpLoactionSQL2").master("local[*]").getOrCreate()
    //取到HDFS中的ip规则
    import sparkSession.implicits._
    val rulesLines: Dataset[String] = sparkSession.read.textFile(args(0))
    val rluesDataset: Dataset[(Long, Long, String)] = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    })
    //收集ip规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = rluesDataset.collect()
    //广播(必须使用sparkcontext)
    //将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sparkSession.sparkContext.broadcast(rulesInDriver)
    //创建RDD，读取访问日志
    val accessLines: Dataset[String] = sparkSession.read.textFile(args(1))
    //整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      //将log日志的每一行进行切分
      val fields: Array[String] = log.split("[|]")
      //将ip转换成十进制
      val ip: String = fields(1)
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")
    ipDataFrame.createOrReplaceTempView("v_log")
    //定义一个自定义函数（UDF），并注册
    //该函数的功能是（输入一个IP地址对应的十进制，返回一个省份名称）
    sparkSession.udf.register("ip2Province",(ipNum:Long) =>{
      //查找ip规则（事先已经广播了，已经在Executor中了）
      //函数的逻辑是在Executor中执行的，怎样获取ip规则的对应的数据呢？
      //使用广播变量的引用，就可以获得
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //根据IP地址对应的十进制查找省份名称
      val index: Int = MyUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if(-1 !=  index){
        province =ipRulesInExecutor(index)._3
      }
      province
    })
    //执行SQL
    val result: DataFrame = sparkSession.sql("select ip2Province(ip_num) province, COUNT(*) counts from v_log group by province order by counts DESC")
    result.show()
    sparkSession.stop()
  }
}
