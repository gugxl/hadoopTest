package com.gugu.spark.day10

import com.gugu.spark.day04.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object CalculateUtil {
  def calculateIncome(fields: RDD[Array[String]]) ={
    //将数据计算后写入到Reids
    val priceRDD: RDD[Double] = fields.map(arr => {
      val price: Double = arr(4).toDouble
      price
    })
    //reduce是一个Action，会把结果返回到Driver端
    //reduce是一个Action，会把结果返回到Driver端
    val sum: Double = priceRDD.reduce(_+_)
    //获取一个jedis连接
    val conn: Jedis = JedisConnectionPool.getConnection()
    //将历史值和当前的值进行累加
//    conn.set(Constant.TOTAL_INCOME, sum.toString)
    conn.incrByFloat(Constant.TOTAL_INCOME, sum)
    //释放连接
    conn.close()
  }
  //  计算分类的成交金额
  def calculateItem(fields: RDD[Array[String]]) ={
    //对field的map方法是在哪一端调用的呢？Driver
    val itemAndPrice: RDD[(String, Double)] = fields.map(arr => {
      //分类
      val item: String = arr(2)
      //金额
      val price: Double = arr(4).toDouble
      (item, price)
    })

    //按照商品分类进行聚合
    val reduced: RDD[(String, Double)] = itemAndPrice.reduceByKey(_+_)
    //将当前批次的数据累加到Redis中
    //foreachPartition是一个Action
    //现在这种方式，jeids的连接是在哪一端创建的（Driver）
    //在Driver端拿Jedis连接不好
    //val conn = JedisConnectionPool.getConnection()
    reduced.foreachPartition(part =>{
      //获取一个Jedis连接
      //这个连接其实是在Executor中的获取的
      //JedisConnectionPool在一个Executor进程中有几个实例（单例）
      val conn: Jedis = JedisConnectionPool.getConnection()
      part.foreach(t => {
        //一个连接更新多条数据
        conn.incrByFloat(t._1,t._2)
      })
      //将当前分区中的数据跟新完在关闭连接
      conn.close()
    })
  }
  //根据Ip计算归属地
  def calculateZone(fields: RDD[Array[String]],broadcastRef: Broadcast[Array[(Long,Long,String)]]) ={
    val provinceAndPrice: RDD[(String, Double)] = fields.map(arr => {
      val ip: String = arr(1)
      val price: Double = arr(4).toDouble
      val ipNum: Long = MyUtils.ip2Long(ip)
      //在Executor中获取到广播的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value
      //二分法查找
      val index: Int = MyUtils.binarySearch(allRules, ipNum)
      var province = "未知"
      if (index != -1) {
        province = allRules(index)._3
      }
      //省份，订单金额
      (province, price)
    })
    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)
    //将数据跟新到Redis
    reduced.foreachPartition(part => {
      val conn: Jedis = JedisConnectionPool.getConnection()
      part.foreach(t => {
        conn.incrByFloat(t._1,t._2)
      })
      conn.close()
    })
  }

}
