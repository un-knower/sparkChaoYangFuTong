package com.ald.stat.job.patch

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AbstractBaseJob
import com.ald.stat.job.task.ShareAllTask
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object AnalysisSharePatch extends AbstractBaseJob {

  val prefix = "jobShare"
  val offsetPrefix = "share_offset"
  val baseRedisKey = "rt";

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    var partitionNumber = 20
    var dateStr = ComputeTimeUtils.getDateStr(new Date())
    var patchType = GlobalConstants.PATCH_TYPE_CACHE_LATEST
    if (args.length >= 1) {
      if (args(0).toInt > 0) {
        try {
          partitionNumber = args(0).toInt
        } catch {
          case e: NumberFormatException => {
            println(s"$args(0)")
            e.printStackTrace()
          }
        }
      }
      if (args.length >= 2) {
        dateStr = args(1)
      }
      //判断补偿逻辑
      if (args.length >= 3) {
        try {
          patchType = args(2)
        } catch {
          case e: NumberFormatException => {
            println(s"$args(2)")
            e.printStackTrace()
          }
        }
      }
    }

    println(s"$partitionNumber,$dateStr,$patchType")

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //                  .set("spark.executor.cores", "2")
      //      .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
    //rdd patch
    rddPatch(sparkConf, taskId, dateStr, partitionNumber, patchType)
  }


  /**
    *
    * @param sparkConf
    * @param partitionNumber
    */
  def rddPatch(sparkConf: SparkConf, taskId: String, dateStr: String, partitionNumber: Int, patchType: String): Unit = {

    val sparkContext = new SparkContext(sparkConf)
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val grey_kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("grey_kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val redisPrefix = sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")

    //清理缓存 删除业务相关的key(如：pv,uv，session的key)
    //RedisUtils.clearKeys(dateStr, GlobalConstants.JOB_REDIS_KEY_SHARE, prefix)
    //上次工作对key关键字
    RedisUtils.clearKeys(dateStr, GlobalConstants.REDIS_KEY_STEP, offsetPrefix)
    RedisUtils.clearKeys(dateStr, GlobalConstants.DONE_PARTITIONS, offsetPrefix)
    if (patchType == GlobalConstants.PATCH_TYPE_REAL_LATEST) {
      //将实时代码设置的offset清理，之后，则将补偿下限到当前最大到offset
      RedisUtils.clearKeys(dateStr, GlobalConstants.REDIS_KEY_LATEST_OFFSET, offsetPrefix)
    }

    var loop = 0
    breakable {
      while (true) {
        loop = loop + 1
        println("loop:" + loop)
        val rddAndExit = KafkaConsume.rddFromOffsetRange(sparkContext, topic, group, baseRedisKey, offsetPrefix)
        //退出
        if (rddAndExit._2) {
          break()
        }
        val rdd: RDD[ConsumerRecord[String, String]] = rddAndExit._1
        rdd.cache()
        handleNewUserForOldSDK(rddAndExit._1, baseRedisKey, prefix)
        //click rdd
        val logRecord_click_rdd: RDD[LogRecord] = rdd.repartition(partitionNumber).mapPartitions(par => {
          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val offsetRedisCache = CacheClientFactory.getInstances(offsetPrefix).asInstanceOf[ClientRedisCache]
          val resource = redisCache.getResource
          val resource_offset = offsetRedisCache.getResource

          val dateStr = ComputeTimeUtils.getDateStr(new Date())
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            par.foreach(line => {
              RddUtils.checkAndSaveOffset(dateStr, line.topic(), group, line, resource_offset)
              //维护offset
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
                  recordsRdd += logRecord
                }
              }
            })
          }catch {
            case jce: JedisConnectionException => jce.printStackTrace()
          } finally {
            if (resource_offset != null) resource_offset.close()
            if (resource != null) resource.close()
            if (redisCache != null) redisCache.close()
            if (offsetRedisCache != null) offsetRedisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val logRecord_status_rdd: RDD[LogRecord] = rdd.repartition(partitionNumber).mapPartitions(par => {
          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try {
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
                      recordsRdd += logRecord
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val records_rdd_userShare_click: RDD[LogRecord] = rdd.repartition(partitionNumber).mapPartitions(par => {
          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try {
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
                      recordsRdd += logRecord
                    }
                  }
                })
            }
          }finally {
            if (redisCache != null) redisCache.close()
          }
          recordsRdd.iterator
        }).cache()

        val records_rdd_userShare_status = rdd.repartition(partitionNumber).mapPartitions(par => {
          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val recordsRdd = ArrayBuffer[LogRecord]()
          try {
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
                    //只需要ev = event 记录
                    if (logRecord.ev == "event" && StringUtils.isNotBlank(logRecord.src)) {
                      recordsRdd += logRecord
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
        //结束后移除
        rdd.unpersist(true)
      }
    }
  }

}
