package com.gugu.spark

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.concurrent.duration._

/**
 * 模拟sparkMaster和SparkWorker
 */
class SparkMaster extends Actor{
  // 存储worker的信息的
  private val workersMap: mutable.HashMap[String, WorkerInfo] = mutable.HashMap[String,WorkerInfo]()
  override def receive: Receive = {
    // 收到worker注册过来的信息
    case RegisterWorkerInfo(wkId,core,ram) => {
      // 将worker的信息存储起来，存储到HashMap
      if(!workersMap.contains(wkId)){
//        workersMap.put(wkId, new WorkerInfo(wkId,core,ram)) // 两种写法
        workersMap += ((wkId,new WorkerInfo(wkId,core,ram)))
        // master存储完worker注册的数据之后，要告诉worker说你已经注册成功
        sender() ! RegisteredWorkerInfo // 此时worker会收到注册成功消息
      }
    }
    case HearBeat(wkId)=>{
      // master收到worker的心跳消息之后，更新woker的上一次心跳时间
      val workerInfo: WorkerInfo = workersMap(wkId)
      // 更改心跳时间
      workerInfo.lastHeartBeatTime = System.currentTimeMillis()
    }
    case CheckTimeOutWorker => {
      import context.dispatcher // 使用调度器时候必须导入dispatcher
      context.system.scheduler.schedule(0 millis, 6000 millis, self, RemoveTimeOutWorker)
    }
    case RemoveTimeOutWorker =>{
      // 将hashMap中的所有的value都拿出来，查看当前时间和上一次心跳时间的差 3000
      val workersInfo: Iterable[WorkerInfo] = workersMap.values
      val currentTime: Long = System.currentTimeMillis()
      //  过滤超时的worker
      workersInfo.filter(wkInfo => currentTime - wkInfo.lastHeartBeatTime > 3000)
        .foreach(wk => workersMap.remove(wk.id))
      println(s"-----还剩 ${workersMap.size} 存活的Worker-----")
    }
  }
}
object SparkMaster{
  private var name = ""
  private val age = 100

  def main(args: Array[String]): Unit = {
    // 检验参数
    if(args.length != 3){
      println(
        """
          |请输入参数：<host> <port> <masterName>
                """.stripMargin)
      sys.exit(-1)// 退出程序
    }
    val host = args(0)
    val port = args(1)
    val masterName = args(2)
    val config: Config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
            """.stripMargin)
    val sparkMaster: ActorSystem = ActorSystem("sparkMaster",config)
    val masterRef: ActorRef = sparkMaster.actorOf(Props[SparkMaster],masterName)
    // 自己给自己发送一个消息，去启动一个调度器，定期的检测HashMap中超时的worker
    masterRef ! CheckTimeOutWorker
  }
}