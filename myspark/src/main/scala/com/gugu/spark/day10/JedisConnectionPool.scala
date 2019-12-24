package com.gugu.spark.day10


import java.util
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  private val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "192.168.2.100", 6379, 10000)
  def getConnection() ={
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn: Jedis = JedisConnectionPool.getConnection()
    conn.set("gugu","25")
    val value1: String = conn.get("gugu")
    println(value1)
    conn.incrBy("gugu",10)
    val value2: String = conn.get("gugu")
    println(value2)
    val keys: util.Set[String] = conn.keys("*")
    import scala.collection.JavaConversions._
    for(key <- keys){
      println(s"key:${key},value:${conn.get(key)}")
    }


    conn.close()



  }
}
