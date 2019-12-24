package com.gugu.spark.day09

import java.lang

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, StreamingContext}


object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    //指定组名
    val group = "g001"
    val conf: SparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[*]")
    //创建SparkConf
    val ssc = new StreamingContext(conf,Duration(5))
    //指定消费的 topic 名字
    val topic = "my-topic"
    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "master:9092,slave1:9092,slave2:9092"

    //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
    val topics: Set[String] = Set(topic)

    //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量

    val topicDirs = new ZKGroupTopicDirs(group,topic)
    //获取 zookeeper 中的路径 "/g001/offsets/wordcount/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //准备kafka的参数
    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> brokerList,
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id" -> group,
//      //从头开始读取数据
//      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
      "auto.offset.reset"-> "latest",
      "enable.auto.commit"->(false: java.lang.Boolean)
    )
    //zookeeper 的host 和 ip，创建一个 client,用于跟新偏移量量的
    //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    // /g001/offsets/wordcount/0/10001"
    // /g001/offsets/wordcount/1/30001"
    // /g001/offsets/wordcount/2/10001"
    //zkTopicPath  -> /g001/offsets/wordcount/
    val children: Int = zkClient.countChildren(zkTopicPath)
    var kafkaStream:InputDStream[ConsumerRecord[String,String]] = null

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffSets: Map[TopicPartition, Long] = Map()
    //如果保存过 offset
    if(children > 0){
      for(i <- 0 until children){
        // /g001/offsets/wordcount/0/10001

        // /g001/offsets/wordcount/0
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wordcount/0
        val tp = new TopicPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wordcount/0 -> 10001
        fromOffSets += (tp -> partitionOffset.toLong)
      }
      //Key: kafka的key   values: "hello tom hello jerry"
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple
//      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String,String]) => (mmd.key(),mmd.message())
//      0.8.0
//      //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
//      //[String, String, StringDecoder, StringDecoder,     (String, String)]
//      //  key    value    key的解码方式   value的解码方式

//      kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,kafkaParams,fromOffSets,messageHandler)
//      新版本

      kafkaStream = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,kafkaParams,fromOffSets)
      )
    }else{
      kafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent,Subscribe[String,String](topics,kafkaParams))
    }
    //偏移量的范围
    var offersetRangs: Array[OffsetRange] = Array[OffsetRange]()

    //从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
    //该transform方法计算获取到当前批次RDD,然后将RDD的偏移量取出来，然后在将RDD返回到DStream

    val transform= kafkaStream.transform {
      rdd =>
        //得到该 rdd 对应 kafka 的消息的 offset
        //该RDD是一个KafkaRDD，可以获得偏移量的范围
        offersetRangs = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }
    val messages: DStream[String] = transform.map(_.value())

    //依次迭代DStream中的RDD
    messages.foreachRDD{
      rdd =>
      //对RDD进行操作，触发Action
        rdd.foreachPartition(partition =>
          partition.foreach(x => {println(x)})
        )
        for (o <- offersetRangs){
          //  /g001/offsets/wordcount/0
          s"${topicDirs.consumerOffsetDir}/${o.partition}"
          //将该 partition 的 offset 保存到 zookeeper
          //  /g001/offsets/wordcount/0/20000

          ZkUtils(zkClient,false).updatePersistentPath(zkTopicPath,o.untilOffset.toString)
        }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
