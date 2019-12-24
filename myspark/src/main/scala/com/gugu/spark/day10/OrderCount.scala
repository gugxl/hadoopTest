package com.gugu.spark.day10

import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.{Duration, StreamingContext}
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object OrderCount {
  def main(args: Array[String]): Unit = {
    //指定组名
    val group = "g001"
    //创建SparkConf
    val conf = new SparkConf().setAppName("OrderCount").setMaster("local[*]")
    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Duration(5))
    val broadcastRef = IPUtils.broadcastIpRules(ssc,"file:///D:\\logs\\ip\\ip.txt")
    //指定消费的 topic 名字
    val topic = "my-topic"
    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "master:9092,slave1:9092,slave2:9092"
    //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
    val topics: Set[String] = Set(topic)

    //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //获取 zookeeper 中的路径 "/g001/offsets/wordcount/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //准备kafka的参数
    val kafkaParams: Map[String, Object] = Map(
//      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      "bootstrap.servers"->brokerList,
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->group,

      //从头开始读取数据
//      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
      "auto.offset.reset"-> "latest",
      "enable.auto.commit"->(false: java.lang.Boolean)
    )
    //zookeeper 的host 和 ip，创建一个 client,用于更新偏移量量的
    //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    // /g001/offsets/wordcount/0/10001"
    // /g001/offsets/wordcount/1/30001"
    // /g001/offsets/wordcount/2/10001"
    //zkTopicPath  -> /g001/offsets/wordcount/
    val children: Int = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[ConsumerRecord[String,String]] = null
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicPartition, Long] = Map()
    //如果保存过 offset
    //注意：偏移量的查询是在Driver完成的
    if(children > 0){
      for(i <- 0 until children){
        // /g001/offsets/wordcount/0/10001
        // /g001/offsets/wordcount/0
        val partitionOffset: String = zkClient.readData[String](s"${zkTopicPath}/${i}")
        // wordcount/0
        val tp = new TopicPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wordcount/0 -> 10001
        fromOffsets += ( tp -> partitionOffset.toLong)
      }
      //Key: kafka的key   values: "hello tom hello jerry"
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String,String]) => (mmd.key(), mmd.message())
      //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
      //[String, String, StringDecoder, StringDecoder,     (String, String)]
      //  key    value    key的解码方式   value的解码方式
      kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        //订阅的策略
        Subscribe[String, String](topics, kafkaParams,fromOffsets)
      )
    }else{
      kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        //订阅的策略
        Subscribe[String, String](topics, kafkaParams)
      )
      //偏移量的范围
      var offsetRanges = Array[OffsetRange]()
      //直连方式只有在KafkaDStream的RDD（KafkaRDD）中才能获取偏移量，那么就不能到调用DStream的Transformation
      //所以只能子在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
      //依次迭代KafkaDStream中的KafkaRDD
      //如果使用直连方式累加数据，那么就要在外部的数据库中进行累加（用KeyVlaue的内存数据库（NoSQL），Redis）
      //kafkaStream.foreachRDD里面的业务逻辑是在Driver端执行
      kafkaStream.foreachRDD{
        kafkaRdd =>
        //判断当前的kafkaStream中的RDD是否有数据
        if(!(kafkaRdd.isEmpty())){
          //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
          val offsetRanges: Array[OffsetRange] = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges
          val lines: RDD[String] = kafkaRdd.map(_.value())
          //整理数据
          val fields: RDD[Array[String]] = lines.map(_.split(" "))
          //计算成交总金额
          CalculateUtil.calculateIncome(fields)

          //计算商品分类金额
          CalculateUtil.calculateItem(fields)

          //计算区域成交金额
          CalculateUtil.calculateZone(fields, broadcastRef)

          //偏移量跟新在哪一端（）
          for(o <- offsetRanges){
            //  /g001/offsets/wordcount/0
            val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
            //将该 partition 的 offset 保存到 zookeeper
            //  /g001/offsets/wordcount/0/20000
            zkClient.writeData(zkPath, o.untilOffset.toString.getBytes())
          }

        }
      }
    }
    kafkaStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
