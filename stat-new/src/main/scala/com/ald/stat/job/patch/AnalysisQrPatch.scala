package com.ald.stat.job.patch

import java.util
import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AbstractBaseJob
import com.ald.stat.job.task.QrAllTask
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object AnalysisQrPatch extends AbstractBaseJob {

  val prefix = "jobQr"
  val offsetPrefix = "qr_offset"
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
      .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
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
    //RedisUtils.clearKeys(dateStr, GlobalConstants.JOB_REDIS_KEY_QR, prefix)
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
        handleNewUserForOldSDK(rddAndExit._1, baseRedisKey, prefix)

        val logRecordRdd: RDD[LogRecord] = rddAndExit._1.repartition(partitionNumber).mapPartitions(par => {
          val dateStr = ComputeTimeUtils.getDateStr(new Date())
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
          val qrMap = new util.HashMap[String, String]()
          use(getConnection()) { conn =>
            use(conn.createStatement()) {
              statement =>
                val rs = statement.executeQuery(
                  """
                    |select qr_key, qr_group_key
                    |from ald_code
                  """.stripMargin)
                while (rs.next()) {
                  val qr_key = rs.getString(1)
                  val qr_group_key = rs.getString(2)
                  if (qr_key != null && qr_group_key != null) {
                    qrMap.put(qr_key.toString, qr_group_key.toString)
                  }
                }
            }
          }

          val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
          val offsetRedisCache = CacheClientFactory.getInstances(offsetPrefix).asInstanceOf[ClientRedisCache]
          val resource = redisCache.getResource
          val resource_offset = offsetRedisCache.getResource

          val recordsRdd = ArrayBuffer[LogRecord]()
          try{
            par.foreach(line => {
              RddUtils.checkAndSaveOffset(dateStr, line.topic(), group, line, resource_offset)
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.pp) &&
                StringUtils.isNotBlank(logRecord.qr)
              ) {
                logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
                //把新用户的会话中的所有记录都标记为ifo=true
                markNewUser(logRecord, resource, baseRedisKey)

                var qr_geroup_key = qrMap.get(logRecord.qr.trim)
                if (StringUtils.isNotBlank(qr_geroup_key)) {
                  logRecord.qr_group = qr_geroup_key
                } else {
                  qr_geroup_key = "其它"
                }
                recordsRdd += logRecord
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

        val grap_map = cacheGrapUser("online_status_qr") //缓存灰度用户上线状态
        val isOnLine = isOnline("online_status_qr")

        QrAllTask.qrDailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        QrAllTask.qrHourStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        QrAllTask.qrGroupDailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
        QrAllTask.qrGroupHourStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
      }
    }
  }

}
