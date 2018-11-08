package com.ald.stat.job

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AnalysisTrend._
import com.ald.stat.job.task._
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink, RddUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 地域分析
  */
object AnalysisRegion {

  val prefix = "jobRegion"
  lazy val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
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
        val recordsRdd = ArrayBuffer[LogRecord]()
        use(redisCache.getResource) {
          resource =>
            par.foreach(line => {
              RddUtils.checkAndSaveOffset(dateStr, line.topic(), group, line, resource)
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.dr) &&
                StringUtils.isNotBlank(logRecord.pp) &&
                StringUtils.isNotBlank(logRecord.city) &&
                StringUtils.isNotBlank(logRecord.province)) {
                if (logRecord.v != null && logRecord.v < "7.0.0") {
                  markNewUser(logRecord, resource, baseRedisKey)
                }
                recordsRdd += logRecord
              }
            })
        }
        recordsRdd.iterator
    }).cache()

    val grap_map = cacheGrapUser("online_status_regional") //缓存灰度用户上线状态
    val isOnLine = isOnline()
    //城市
    DailyCityTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
    //省份
    DailyProvinceTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
  })
  ssc.start()
  ssc.awaitTermination()
  }
}
