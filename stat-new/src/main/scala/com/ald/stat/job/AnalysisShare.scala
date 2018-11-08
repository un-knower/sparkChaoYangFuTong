package com.ald.stat.job

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.task._
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object AnalysisShare extends AbstractBaseJob {

  val prefix = "jobShare"
  val offsetPrefix = "share_offset"
  val baseRedisKey = "rt";

  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    var partitionNumber = 20
    if (args.length >= 1) {
      if (args(0).toInt > 0) {
        partitionNumber = args(0).toInt
      }
    }

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //      .set("spark.executor.cores", "10")
//            .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val grey_kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("grey_kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


    //广播
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    //val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, offsetPrefix)

    /**维护最小的offset*/
    stream.foreachRDD(rdd=>{
      rdd.foreachPartition(par=>{
        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        val offsetRedisCache = CacheClientFactory.getInstances(offsetPrefix).asInstanceOf[ClientRedisCache]
        val resource_offset = offsetRedisCache.getResource
        try{
          val first_record_iterator = par.take(1)//获取每个分区中的第一个元素作为最小的offset的元素
          first_record_iterator.foreach(record=>{
            RddUtils.checkAndSaveMinOffset(dateStr, record.topic(), group, record, resource_offset)
          })
        }finally {
          if (resource_offset != null) resource_offset.close()
          if (offsetRedisCache != null) offsetRedisCache.close()
        }
      })
    })

    /**标记新用户并且维护最大的offset*/
    handleNewUserAndLatestOffset(stream, group,baseRedisKey, prefix,offsetPrefix)

    stream.repartition(partitionNumber).foreachRDD(rdd => {
        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())

        //分享概况和页面分享的数据
        val logRecord_click_rdd = rdd.mapPartitions(par => {
            val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
            val resource = redisCache.getResource
            val recordsRdd = ArrayBuffer[LogRecord]()
            try{
              par.foreach(line => {
                val logRecord = LogRecord.line2Bean(line.value())
                if (logRecord != null &&
                  logRecord.ev != "app" &&
                  StringUtils.isNotBlank(logRecord.ak) &&
                  StringUtils.isNotBlank(logRecord.at) &&
                  StringUtils.isNotBlank(logRecord.ev) &&
                  StringUtils.isNotBlank(logRecord.uu) &&
                  StringUtils.isNotBlank(logRecord.et) &&
                  StringUtils.isNotBlank(logRecord.tp) &&
                  StringUtils.isNotBlank(logRecord.path) &&
                  logRecord.tp == "ald_share_click"
                ) {
                  logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                  //将path赋给pp,防止session计算报错
                  logRecord.pp = logRecord.path
                  //把新用户的会话中的所有记录都标记为ifo=true
                  markNewUser(logRecord, resource, baseRedisKey)
                  //只需要 ev = event
                  if (logRecord.ev == "event") {
                    if(logRecord.v >= "7.0.0"){
                      if(logRecord.ev != "app"){
                        recordsRdd += logRecord
                      }
                    }else{
                      recordsRdd += logRecord
                    }
                  }
                }
              })
            }catch {
              case jce: JedisConnectionException => jce.printStackTrace()
            } finally {
              if (resource != null) resource.close()
              if (redisCache != null) redisCache.close()
            }

            recordsRdd.iterator
          }).cache()

        val logRecord_status_rdd = rdd.mapPartitions(par => {
          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            use(redisCache.getResource) {
              resource =>
                par.foreach(line => {
                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    logRecord.ev != "app" &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_status"
                  ) {
                    logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path
                    //把新用户的会话中的所有记录都标记为ifo=true
                    markNewUser(logRecord, resource, baseRedisKey)
                    //只需要 ev = event
                    if (logRecord.ev == "event") {
                      if(logRecord.v >= "7.0.0"){
                        if(logRecord.ev != "app"){
                          recordsRdd += logRecord
                        }
                      }else{
                        recordsRdd += logRecord
                      }
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        //用户分享所需数据
        val records_rdd_userShare_click = rdd.mapPartitions(par => {
          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            use(redisCache.getResource) {
              resource =>
                par.foreach(line => {
                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    logRecord.ev != "app" &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_click"
                  ) {
                    logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path
                    //把新用户的会话中的所有记录都标记为ifo=true
                    markNewUser(logRecord, resource, baseRedisKey)
                    //从wsr_query_ald_share_src中获取分享源，取最后一个作为分享用户
                    if (StringUtils.isNotBlank(logRecord.wsr_query_ald_share_src)) {
                      val src_arr = logRecord.wsr_query_ald_share_src.split(",")
                      if (src_arr.length >= 1) {
                        logRecord.src = src_arr(src_arr.length - 1) //取最后一个uu，做为分享源
                      }
                    }

                    //只需要ev = event 记录
                    if (logRecord.ev == "event" && StringUtils.isNotBlank(logRecord.src)) {
                      if(logRecord.v >= "7.0.0"){
                        if(logRecord.ev != "app"){
                          recordsRdd += logRecord
                        }
                      }else{
                        recordsRdd += logRecord
                      }
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val records_rdd_userShare_status = rdd.mapPartitions(par => {
          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            use(redisCache.getResource) {
              resource =>
                par.foreach(line => {
                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    logRecord.ev != "app" &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_status"
                  ) {
                    logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path
                    //把新用户的会话中的所有记录都标记为ifo=true
                    markNewUser(logRecord, resource, baseRedisKey)
                    //从wsr_query_ald_share_src中获取分享源，取最后一个作为分享用户
                    if (StringUtils.isNotBlank(logRecord.wsr_query_ald_share_src)) {
                      val src_arr = logRecord.wsr_query_ald_share_src.split(",")
                      if (src_arr.length >= 1) {
                        logRecord.src = src_arr(src_arr.length - 1) //取最后一个uu，做为分享源
                      }
                    }
                    //logRecord.src = logRecord.uu
                    //只需要ev = event 记录
                    if (logRecord.ev == "event" && StringUtils.isNotBlank(logRecord.src)) {
                      if(logRecord.v >= "7.0.0"){
                        if(logRecord.ev != "app"){
                          recordsRdd += logRecord
                        }
                      }else{
                        recordsRdd += logRecord
                      }
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val grap_map = cacheGrapUser("online_status_share") //缓存灰度用户上线状态
        val isOnLine = isOnline("online_status_share") //上线开关

        //分享概况
        ShareAllTask.shareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        ShareAllTask.shareHourStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        //页面分享
        ShareAllTask.pageShareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        //用户分享
        ShareAllTask.userShareDailyStat(baseRedisKey, taskId, dateStr, records_rdd_userShare_click, records_rdd_userShare_status, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
