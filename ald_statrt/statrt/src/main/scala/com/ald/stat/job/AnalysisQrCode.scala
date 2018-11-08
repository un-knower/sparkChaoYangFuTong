package com.ald.stat.job

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.task._
import com.ald.stat.kafka.hbase.SQLKafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AnalysisQrCode {

  val prefix = "jobQr"
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

    val qrRedisKey = "rt:qr"
    val resource = redisCache.getResource
    val sceneMap = resource.hgetAll(qrRedisKey)
    if (sceneMap == null || sceneMap.isEmpty) {
      println("初始化场景值")
      use(redisCache.getResource) {
        resource =>
          use(getConnection()) { conn =>
            use(conn.createStatement()) {
              statement =>
                val rs = statement.executeQuery("select qr_key, qr_group_key from ald_code")
                while (rs.next()) {
                  val qr_key = rs.getString(1)
                  val qr_group_key = rs.getString(2)
                  if (qr_key != null && qr_group_key != null) {
                    sceneMap.put(qr_key.toString, qr_group_key.toString)
                    resource.hset(qrRedisKey, qr_key.toString, qr_group_key.toString)
                  }
                }
            }
          }
      }
    }
    //广播变量
    val qrGroupMap = ssc.sparkContext.broadcast(sceneMap)

    //广播
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    val stream = SQLKafkaConsume.getStream(ssc, group, topic)

    stream.repartition(partitionNumber)
      .foreachRDD(rdd => {
        val logRecordRdd = rdd.mapPartitions(
          par => {
            val recordsRdd = ArrayBuffer[LogRecord]()
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.qr) // TODO: 过滤qr字段，可能会导致“app”级别的日志被过滤，导致新用户统计结果为 0 ；
              ) {

                /*if (logRecord.et == null) {
                  logRecord.et = logRecord.st
                }*/

                val qr_geroup_key = qrGroupMap.value.get(logRecord.qr.trim)
                if (StringUtils.isNotBlank(qr_geroup_key)) {
                  logRecord.qr_group = qr_geroup_key
                }
                recordsRdd += logRecord
              }
            })
            recordsRdd.iterator
          }).cache()

        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        QrAllTask.qrDailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
        QrAllTask.qrHourStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
        QrAllTask.qrGroupDailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
        QrAllTask.qrGroupHourStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

      })
    ssc.start()
    ssc.awaitTermination()
  }

}
