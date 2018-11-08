package com.ald.stat.job

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.task._
import com.ald.stat.kafka.hbase.SQLKafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.use
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AnalysisShare {

  val prefix = "jobShare"
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


    //广播
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    val stream = SQLKafkaConsume.getStream(ssc, group, topic)

    val shareRedisKey = "rt:share"

    /** 把新用户的at缓存起来 */
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        use(redisCache.getResource) {
          resource =>
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null && logRecord.ifo == "true") {
                resource.hset(shareRedisKey, logRecord.at, "true")
              }
            })
        }
      })
    })

    stream.repartition(partitionNumber)
      .foreachRDD(rdd => {
        /** 分别过滤出click 和 status 的数据
          * 这样性能可能很差
          */
        val logRecord_click_rdd = rdd.mapPartitions(
          par => {
            val recordsRdd = ArrayBuffer[LogRecord]()
            use(redisCache.getResource) {
              resource =>
                par.foreach(line => {
                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_click"
                  ) {
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path

                    //把新用户的会话中的所有记录都标记为ifo=true
                    val ifo = resource.hget(shareRedisKey, logRecord.at.trim)
                    if (StringUtils.isNotBlank(ifo)) {
                      logRecord.ifo = ifo
                    }

                    //只需要 ev = event
                    if (logRecord.ev == "event") {
                      recordsRdd += logRecord
                    }
                  }
                })
            }
            recordsRdd.iterator
          }).cache()

        val logRecord_status_rdd = rdd.mapPartitions(
          par => {
            val recordsRdd = ArrayBuffer[LogRecord]()
            use(redisCache.getResource) {
              resource =>
                par.foreach(line => {
                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_status"
                  ) {
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path

                    //把新用户的会话中的所有记录都标记为ifo=true
                    val ifo = resource.hget(shareRedisKey, logRecord.at.trim)
                    if (StringUtils.isNotBlank(ifo)) {
                      logRecord.ifo = ifo
                    }

                    //只需要 ev = event
                    if (logRecord.ev == "event") {
                      recordsRdd += logRecord
                    }
                  }
                })
            }
            recordsRdd.iterator
          }).cache()

        /** 清洗出用户分享所需数据 */
        val records_rdd_userShare_click = rdd.mapPartitions(
          par => {
            val recordsRdd = ArrayBuffer[LogRecord]()
            use(redisCache.getResource) {
              resource =>
                par.foreach(line => {
                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_click"
                  ) {
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path

                    //把新用户的会话中的所有记录都标记为ifo=true
                    val ifo = resource.hget(shareRedisKey, logRecord.at.trim)
                    if (StringUtils.isNotBlank(ifo)) {
                      logRecord.ifo = ifo
                    }

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
            recordsRdd.iterator
          }).cache()
        val records_rdd_userShare_status = rdd.mapPartitions(
          par => {
            val recordsRdd = ArrayBuffer[LogRecord]()
            use(redisCache.getResource) {
              resource =>
                par.foreach(line => {
                  val logRecord = LogRecord.line2Bean(line.value())
                  if (logRecord != null &&
                    StringUtils.isNotBlank(logRecord.ak) &&
                    StringUtils.isNotBlank(logRecord.at) &&
                    StringUtils.isNotBlank(logRecord.ev) &&
                    StringUtils.isNotBlank(logRecord.uu) &&
                    StringUtils.isNotBlank(logRecord.et) &&
                    StringUtils.isNotBlank(logRecord.tp) &&
                    StringUtils.isNotBlank(logRecord.path) &&
                    logRecord.tp == "ald_share_status"
                  ) {
                    //将path赋给pp,防止session计算报错
                    logRecord.pp = logRecord.path

                    //把新用户的会话中的所有记录都标记为ifo=true
                    val ifo = resource.hget(shareRedisKey, logRecord.at.trim)
                    if (StringUtils.isNotBlank(ifo)) {
                      logRecord.ifo = ifo
                    }

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
            recordsRdd.iterator
          }).cache()

        println("logRecord_click_rdd:" + logRecord_click_rdd.count())
        println("logRecord_status_rdd:" + logRecord_status_rdd.count())

        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        ShareAllTask.shareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, redisPrefix)
        ShareAllTask.shareHourStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, redisPrefix)
        ShareAllTask.pageShareDailyStat(baseRedisKey, taskId, dateStr, logRecord_click_rdd, logRecord_status_rdd, kafkaProducer, redisPrefix)
        ShareAllTask.userShareDailyStat(baseRedisKey, taskId, dateStr, records_rdd_userShare_click, records_rdd_userShare_status, kafkaProducer, redisPrefix)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
