package com.ald.stat.job

import java.util
import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AnalysisTrend.{cacheGrapUser, handleNewUserForOldSDK, isOnline, markNewUser}
import com.ald.stat.job.task._
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink, RddUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 机型分析
  */
object AnalysisPhoneModel {

  val prefix = "jobPhone"
  val offsetPrefix = "offset"
  lazy val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
  lazy val offsetRedisCache = CacheClientFactory.getInstances(offsetPrefix).asInstanceOf[ClientRedisCache]
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
      //      .setMaster(ConfigUtils.getProperty("spark.master.host"))
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

    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")

    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, prefix)
    //存储新用户标记
    handleNewUserForOldSDK(stream, baseRedisKey, prefix)

    stream.repartition(partitionNumber).foreachRDD(rdd => {
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val brandMap = new util.HashMap[String, String]() //品牌
        val modelMap = new util.HashMap[String, String]() //型号
        use(getConnection()) { conn =>
          use(conn.createStatement()) {
            statement =>
              val rs = statement.executeQuery(
                """
                  |select uname,brand,name
                  |from phone_model
                """.
                  stripMargin)
              while (rs.next()) {
                val uname = rs.getString(1)
                val brand = rs.getString(2)
                val name = rs.getString(3)
                if (uname != null && brand != null) {
                  brandMap.put(uname.toString, brand.toString)
                }
                if (uname != null && name != null) {
                  modelMap.put(uname.toString, name.toString)
                }
              }
          }
        }

        val recordsRdd = ArrayBuffer[LogRecord]()
        use(redisCache.getResource) { resource =>
          use(offsetRedisCache.getResource) { resource_offset =>
            par.foreach(line => {
              RddUtils.checkAndSaveOffset(dateStr, line.topic(), group, line, resource_offset)
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.dr) &&
                StringUtils.isNotBlank(logRecord.pp) &&
                StringUtils.isNotBlank(logRecord.pm)) {
                if (logRecord.v != null && logRecord.v < "7.0.0") {
                  markNewUser(logRecord, resource, baseRedisKey)
                }

                val brand = brandMap.get(logRecord.pm.trim) //品牌
                if (StringUtils.isNotBlank(brand)) {
                  logRecord.brand = brand
                } else {
                  logRecord.brand = "未知" //如果不存在则把品牌值赋值为"未知"
                }
                val model = resource.get(logRecord.pm.trim) //机型
                if (StringUtils.isNotBlank(model)) {
                  logRecord.model = model
                } else {
                  logRecord.model = "未知" //如果不存在则把机型值赋值为"未知"
                }
                recordsRdd += logRecord
              }
            })
          }
        }
        recordsRdd.iterator
      }).cache()

      val grap_map = cacheGrapUser("online_status_phone") //缓存灰度用户上线状态
      val isOnLine = isOnline()

      DailyModelTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine,redisPrefix) //机型
      DailyBrandTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix) //品牌
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
