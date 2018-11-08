package com.ald.stat.kafka.hbase

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, RedisUtils}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 2018/5/18.
  */
object KafkaConsume {

  val logger = LoggerFactory.getLogger("KafkaConsume")
  //分步执行的批次记录数量
  var patchBatchNum: String = ConfigUtils.getProperty("patch.batch.num")

  /**
    * 实时从最新offset开始
    *
    * @param ssc
    * @param group
    * @param topic
    * @return
    */
  def getStream(ssc: StreamingContext, group: String, topic: String): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Set(topic)

    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    dStream
  }

  /**
    * 从指定offset开始读取kafka当中的数据。
    *
    * @param ssc
    * @param group
    * @param topic
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def streamFromOffsets(ssc: StreamingContext, group: String, topic: String, baseRedisKey: String, redisPrefix: String): InputDStream[ConsumerRecord[String, String]] = {

    //==========================================================1
    /*
    *gcs:
    *创建一个kafka的Param对象
    */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //gcs:获得broker的host地址
    val brokerHost = ConfigUtils.getProperty("kafka.host")
    //gcs:获得今天的时间。并且将今天的时间设置为yyyyMMdd 的时间格式
    val d = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    //gcs:获得一个redisCache的对象。
    lazy val redisCache = CacheClientFactory.getInstances(redisPrefix).asInstanceOf[ClientRedisCache]

    try{
      use(redisCache.getResource) {
        resource => {
          var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions") //gcs:这个partionList的汉所以是什么啊
          if (StringUtils.isBlank(partitionList)) { //gcs:如果检测到partitionList为null，此时就会将这个partitionList赋值为0或者1
            partitionList = "0,1"
          }
          val map = getWithStartOffset(brokerHost, topic, partitionList)
          var topicPartitionToOffset = Map[TopicPartition, Long]()
          if (map != null && !map.isEmpty) {
            map.foreach(k => {
              val topic = k._1.split(",")(0)
              val partition = k._1.split(",")(1)
              val realEndOffset = k._2.split(",")(0) //kafka中最大的offset

              // 获取offset
              val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")
              var cacheOffset = resource.get(latestOffsetKey)
              println(s"latestOffsetKey:$latestOffsetKey")
              val realStartOffset = k._2.split(",")(1) //kafka中最小的offset
              //矫正 offset ->cacheOffset 为空 或cacheOffset 大于 realEndOffset
              if (StringUtils.isBlank(cacheOffset)) {
                cacheOffset = realEndOffset
              } else {
                if (cacheOffset.toLong > realEndOffset.toLong) {
                  cacheOffset = realEndOffset.toString
                }
              }
              resource.set(latestOffsetKey, cacheOffset)
              println(s"pass date:$d,topic:$topic,$group,$partition,current from:$realStartOffset,real from:$cacheOffset")
              topicPartitionToOffset.+=((new TopicPartition(topic, partition.toInt), cacheOffset.toLong))
            })
          } else {
            System.exit(0)
          }
          KafkaUtils.createDirectStream(ssc, PreferConsistent, Assign[String, String](topicPartitionToOffset.keys, kafkaParams, topicPartitionToOffset))
        }
      }
    }finally {
      if (redisCache != null) redisCache.close()
    }

  }

  /**
    * 获取一定offset范围内的kafka当中的数据。并且将这些数据封装成一个RDD。
    * 我们的kafka当中的数据的offset是存储在redis当中的。所以这里要使用baseRedisKey和redisPrefix从redis当中将redis的key提取出来
    *
    * @param sc
    * @param topic
    * @param group
    * @param baseRedisKey redis的基础key
    * @param redisPrefix redis的前缀
    * @return
    */
  def rddFromOffsetRangeToPatch(sc: SparkContext, topic: String, group: String, baseRedisKey: String, redisPrefix: String): RDD[ConsumerRecord[String, String]]= {


    val kafkaParams = new util.HashMap[String, Object]()
    val brokerHost = ConfigUtils.getProperty("kafka.host") //gcs:创建一个broker的host对象

    //gcs:对kafka的一些配置项进行配置
    kafkaParams.put("bootstrap.servers", brokerHost)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", group)
    kafkaParams.put("auto.offset.reset", "earliest")
    kafkaParams.put("enable.auto.commit", (false: java.lang.Boolean))

    //gcs:获得当前的时间
    val d = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    //gcs:这个partitionList是什么含义啊？？？
    var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions")
    if (StringUtils.isBlank(partitionList)) {
      partitionList = "0,1"  //gcs:如果partitionList为null。此时就为其partition赋值为0,1
    }

    //gcs:使用懒加载的方式获得一个redis的对象
    lazy val redisCache = CacheClientFactory.getInstances(redisPrefix).asInstanceOf[ClientRedisCache]
    use(redisCache.getResource) {
      resource => {

        //gcs:这个map就是kafka的partition的map数组。这个map数组中存储这该topic中的每一个partition当中的[min,max]的offset
        val map = getWithStartOffset(brokerHost, topic, partitionList)//获取kafka中的offset信息


        var topicPartitionToOffset = ArrayBuffer[OffsetRange]()
        if (map != null && !map.isEmpty){

          //gcs:将该topic下的每一个partition的min和max的offset
          map.foreach(k => {
            val topic = k._1.split(",")(0)
            val partition = k._1.split(",")(1)
            val realMaxOffset = k._2.split(",")(0)  //gcs:topic下的每一个partition的真实的max的offset
            val realMinOffset = k._2.split(",")(1)  //gcs:topic下的每一个partition的真实的min的offset

            //gcs:将每一个topic的partition的min的offset和max的offset都提取出来
            println("partition:" + partition + ",realMaxOffset:" + realMaxOffset + ",realMinOffset:" + realMinOffset)


            //gcs:从redis中获得我们自己存储的最小的min的offset。这个min的offset是过了凌晨之后的第一个批次的rdd的偏移
            val minOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "min")//缓存中的最小offset的key

            //gcs:从redis中获得我们自己存储在redis当中的最新的offset。这个最新的offset是数据丢失的时候的那一个批次的RDD的offset
            val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")//缓存中的最新offset的key

            //gcs:从redis当中获得，我们存储的最大的offset和最小的offset。然后从我们的kafka的真实的offset和我们的redis中记录的min的offset这段数据补数据
            var patchMinOffset = resource.get(minOffsetKey)//补偿用的最小的offset
            var patchLatestOffset = resource.get(latestOffsetKey)//补偿用的最大的offset

            if(StringUtils.isNotBlank(patchLatestOffset)){
              if (realMaxOffset.toLong > patchLatestOffset.toLong){ //如果真实的最大的offset大于缓存中最大的offset
                patchLatestOffset = realMaxOffset //此时补偿到真实的最大的offset
              }
            }else{
              patchLatestOffset = realMaxOffset
            }


            //gcs:当把最大的offset取出来之后。取redis当中的realOffset
            resource.set(latestOffsetKey,patchLatestOffset)//把补偿后，最大的offset放入缓存
            resource.expireAt(latestOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期



            //gcs:这个offsetRange就是我们的redis的min的offset，和kafka的topic的partition分区max的offset之间读取数据。将这一段数据距离之间的数据，创建一个RDD。去补数据使用
            val r = OffsetRange.create(new TopicPartition(topic, partition.toInt), patchMinOffset.toLong, patchLatestOffset.toLong)
            println(s"pass date:$d,topic:$topic,$group,$partition,from:$patchMinOffset,end:$patchLatestOffset")
            topicPartitionToOffset.+=(r)
          })
        } else {
          System.exit(0)
        }
        val offsetRanges = topicPartitionToOffset.toArray
        KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, PreferConsistent)
      }
    }
  }


  /**
    * 直接进行获取rdd
    *
    * @param sc
    * @param topic
    * @param group
    * @param baseRedisKey
    * @param redisPrefix
    * @return
    */
  def rddFromOffsetRange(sc: SparkContext, topic: String, group: String, baseRedisKey: String, redisPrefix: String): (RDD[ConsumerRecord[String, String]], Boolean) = {

    val kafkaParams = new util.HashMap[String, Object]()
    val brokerHost = ConfigUtils.getProperty("kafka.host")
    kafkaParams.put("bootstrap.servers", brokerHost)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", group)
    kafkaParams.put("auto.offset.reset", "earliest")
    kafkaParams.put("enable.auto.commit", (false: java.lang.Boolean))

    //判断是否需要退出
    val exit = false
    val d = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val donePartitionsKey = RedisUtils.buildDonePartitionsKey(d, topic, group, "donePartitions")
    var partitionList = ConfigUtils.getProperty("kafka.raw.topic.partitions")
    if (StringUtils.isBlank(partitionList)) {
      partitionList = "0,1"
    }

    lazy val redisCache = CacheClientFactory.getInstances(redisPrefix).asInstanceOf[ClientRedisCache]
    try{
      use(redisCache.getResource) {
        resource => {
          var donePartitions = resource.smembers(donePartitionsKey)
          if (donePartitions == null) {
            donePartitions = new util.HashSet[String]()
          }
          //判断是否所有分区都已经处理完毕
          if (partitionList.split(",").size == donePartitions.size()) {
            println("donePartitions：" + donePartitions)
            return (null, true) //如果所有分区都处理完毕了，则返回true,令补偿程序退出
          }
          val map = getWithStartOffset(brokerHost, topic, partitionList)
          var topicPartitionToOffset = ArrayBuffer[OffsetRange]()

          if (map != null && !map.isEmpty) {
            map.filter(k => {
              val partition = k._1.split(",")(1)
              if (!donePartitions.contains(partition.toString)) {
                true
              } else {
                false
              }
            }).foreach(k => {
              val topic = k._1.split(",")(0)
              val partition = k._1.split(",")(1)
              val realMaxOffset = k._2.split(",")(0)
              val realMinOffset = k._2.split(",")(1)
              println("partition:" + partition + ",realMaxOffset:" + realMaxOffset + ",realMinOffset:" + realMinOffset)

              // 获取offset
              //这里需要进行手动分批计算，保存一个step offset，直到最后一个 step offset 和latest offset一样的时候，结束本次partition计算
              val stepLatestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "step:latest")
              val stepLatestOffset = resource.get(stepLatestOffsetKey)
              if (StringUtils.isBlank(patchBatchNum)) {
                patchBatchNum = "500000"
              }
              //上限
              var stepMinOffsetVal = 0l
              //下限
              var stepLatestOffsetVal = 0l

              if (StringUtils.isBlank(stepLatestOffset)) {
                val minOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "min")
                var minOffset = resource.get(minOffsetKey)
                println("cacheMinOffset:" + minOffset)
                if (StringUtils.isBlank(minOffset)) {
                  minOffset = realMinOffset
                  //更新redis
                  resource.set(minOffsetKey, minOffset)
                } else {
                  if (minOffset.toLong < realMinOffset.toLong) {// TODO: 这里有问题 ，真实的最小的offset 大于 cacheMinOffset
                    //minOffset = realMinOffset
                    //更新redis
                    resource.set(minOffsetKey, minOffset)
                  }
                }
                stepMinOffsetVal = minOffset.toLong
              } else {
                stepMinOffsetVal = stepLatestOffset.toLong
              }
              stepLatestOffsetVal = stepMinOffsetVal + patchBatchNum.toLong
              //下限
              val latestOffsetKey = RedisUtils.buildOffsetKey(d, topic, group, partition.toInt, "latest")
              var latestOffset = resource.get(latestOffsetKey)
              if (StringUtils.isBlank(latestOffset)) {
                latestOffset = realMaxOffset
                //矫正
                resource.set(latestOffsetKey, latestOffset)
              } else {
                //TODO:这里存在补偿缓存上限小于下限
                //缓存中的最大offset比真实最小的offset还小,则把缓存offset矫正为最大offset
                if (latestOffset.toLong < realMinOffset.toLong) {
                  latestOffset = realMaxOffset
                  //矫正
                  resource.set(latestOffsetKey, latestOffset)
                }
              }
              //判断是否需要进行退出
              if (stepLatestOffsetVal > latestOffset.toLong) {
                stepLatestOffsetVal = latestOffset.toLong
                //加入完成的列表
                resource.sadd(donePartitionsKey, partition.toString)
                resource.expireAt(donePartitionsKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
              //步位保存到redis
              resource.set(stepLatestOffsetKey, stepLatestOffsetVal.toString)
              resource.expireAt(stepLatestOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              val r = OffsetRange.create(new TopicPartition(topic, partition.toInt), stepMinOffsetVal, stepLatestOffsetVal)
              println(s"pass date:$d,topic:$topic,$group,$partition,from:$stepMinOffsetVal,end:$stepLatestOffsetVal")
              topicPartitionToOffset.+=(r)
            })
          } else {
            System.exit(0)
          }
          val offsetRanges = topicPartitionToOffset.toArray
          (KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, PreferConsistent), exit)
        }
      }
    }finally {
      if (redisCache != null) redisCache.close()
    }

  }

  /**
    *这个函数的作用是用来获得kafka的topic的每一个分区下的真实的每一个partition分区下的offset的信息
    * @param brokerList kafka的brokerList的地址
    * @param topic kafka的topic的名字
    * @param partitionList 这个分区的list是什么含义啊？？？
    * @return 返还一个Map[String,String]的对象
    */
  def getWithStartOffset(brokerList: String, topic: String, partitionList: String): Map[String, String] = {

    val clientId = "GetOffset"
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList) //gcs:将所有的broker信息从一个string的字符串中解析出来

    //gcs:在没有生产者的情况下，获得一个topic的broker的信息
    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, "trendConsume", 10000).topicsMetadata


    //gcs:这是对返回回来的topicsMetadata做了一个是否有错误的检查
    if (topicsMetadata.size != 1 || !topicsMetadata(0).topic.equals(topic)) {
      System.err.println(("Error: no valid topic metadata for topic: %s, " + " probably the topic does not exist, run ").format(topic) +
        "kafka-list-topic.sh to verify")
      System.exit(1)
    }

    //gcs:这里指定partitonOffset是什么意思啊？？？？？
    var topicPartitionToOffset = Map[String, String]()  //gcs:这个topicPartition

    //gcs:我们的生产环境下的kafka的分区都有8个分区。其余的kafka的分区都只有2个分区
    //gcs:这里的作用是获得kafkad的分区。如果返还回来的kafka的分区是一个空字符串的话，就会先按照","号，切分开，在转换为seq
    val partitions =
      if (partitionList == "") {
        topicsMetadata.head.partitionsMetadata.map(_.partitionId)
      } else {
        partitionList.split(",").map(_.toInt).toSeq
      }

    println("partitionList:" + partitionList)

    //gcs:得到了kafka的partiton的分区之后，就遍历每一个kafka的分区
    partitions.foreach { partitionId =>

      //gcs:这一行代码的作用是什么啊????
      val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partitionId)

      partitionMetadataOpt match {
        case Some(metadata) =>
          metadata.leader match {
            case Some(leader) =>
              //gcs:根据我们给的host和port建立一个kafka的consumer对象
              val consumer = new SimpleConsumer(leader.host, leader.port, 10000, 100000, clientId)

              //gcs:这个根据topic和partitionId，获得指向该topic下的partitionId的指针
              val topicAndPartition = TopicAndPartition(topic, partitionId)


              //gcs:获得kafka的该topic下的该partition分区下的偏移
              val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(-1, 2)))

              //gcs:这是获得什么offsets啊？？？
              //gcs:获得topic下的该partition下的真实的offset分区。
              val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets

              //gcs:min和max
              var max, min = 0l
              if (offsets != null) {
                if (offsets.length == 2) {
                  max = offsets(0)  //gcs:max是代表这个管道下的该topic下的该分区下的最大的偏移
                  min = offsets(1)  //gcs:min是代表该管道下的该topic下的该分区下的最小的偏移
                }
              }


              println(s"current: topic:${topicAndPartition.topic},${topicAndPartition.partition},$max,$min")

              topicPartitionToOffset.+=((s"${topicAndPartition.topic},${topicAndPartition.partition}", s"$max,$min"))

              //gcs:这些行代码的含义是什么啊？？？？？
            case None => System.err.println("Error: partition %d does not have a leader. Skip getting offsets".format(partitionId))
          }
        case None => System.err.println("Error: partition %d does not exist".format(partitionId))
      }
    }

    //gcs：topicPartitionToOffset这个对象中存储这该topic下的该每一个分区的真实的最大偏移和最小的偏移。因为一个topic会有
    // 多个分区，每一个分区多会有max的offset和min的offset。topicPartitionToOffset
    topicPartitionToOffset
  }
}