package com.ald.stat.job.patch.patchJob

import java.util
import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AbstractBaseJob
import com.ald.stat.job.AnalysisLink.{cacheGrapUser, handleNewUserAndLatestOffset, isOnline, markNewUser}
import com.ald.stat.job.patch.patchTask.LinkPatchAllTask
import com.ald.stat.job.task.LinkAllTask
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils._
import com.ald.stat.utils.DBUtils.{getConnection, use}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object AnalysisLinkPatch extends AbstractBaseJob {

  val logger = LoggerFactory.getLogger("AnalysisLink")

  val prefix = "jobLink"
  val offsetPrefix = "link_offset"
  val baseRedisKey = "rt";

  /**
    *
    * gcs:这个程序是用来补实时系统的数据的
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    var partitionNumber = 20
    if (args.length >= 1) {
      if (args(0).toInt > 0) {
        partitionNumber = args(0).toInt
      }
    }

    //gcs:设定一个sparkConf对象
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //      .set("spark.executor.cores", "10")
      //      .setMaster(ConfigUtils.getProperty("spark.master.host"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")


    //gcs:创建一个实时的对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))

    //gcs:获得一个kafka的连接的对象
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

    //gcs:获得一个kafka的produce生产者对象
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

    //init scene meta data to redis
    //广播
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)  //gcs:实话讲这里
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id") //gcs:这是kafka存储的group分组
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic") //gcs:这是获得kafka的topic

    //gcs:从kafka的offset的位置，获得一个实时数据流对象
    //val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
    //gcs:从kafka的topic当中将批次流读出来
    val stream = KafkaConsume.streamFromOffsets(ssc, group, topic, baseRedisKey, offsetPrefix)


    /** 维护最小的offset */
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        val offsetRedisCache = CacheClientFactory.getInstances(offsetPrefix).asInstanceOf[ClientRedisCache]
        val resource_offset = offsetRedisCache.getResource
        try {
          val first_record_iterator = par.take(1) //获取每个分区中的第一个元素作为最小的offset的元素
          first_record_iterator.foreach(record => {
            RddUtils.checkAndSaveMinOffset(dateStr, record.topic(), group, record, resource_offset)
          })
        } finally {
          if (resource_offset != null) resource_offset.close()
          if (offsetRedisCache != null) offsetRedisCache.close()
        }
      })
    })

    /** 标记新用户并且维护最大的offset */
    handleNewUserAndLatestOffset(stream, group, baseRedisKey, prefix, offsetPrefix)

    /** 业务操作 */
    stream.repartition(partitionNumber).foreachRDD(rdd => {
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val linkMap = new util.HashMap[String, String]()
        use(getConnection()) { conn =>
          use(conn.createStatement()) {
            statement =>
              val rs = statement.executeQuery(
                """
                  |select link_key,media_id
                  |from ald_link_trace
                  |where is_del=0
                """.
                  stripMargin)
              while (rs.next()) {
                val link_key = rs.getString(1)
                val media_id = rs.getInt(2)
                if (link_key != null && media_id != null) {
                  linkMap.put(link_key.toString, media_id.toString)
                }
              }
          }
        }

        val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
        val resource = redisCache.getResource
        val recordsRdd = ArrayBuffer[LogRecord]()
        try {
          par.foreach(line => {
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.at) &&
              StringUtils.isNotBlank(logRecord.ev) &&
              StringUtils.isNotBlank(logRecord.uu) &&
              StringUtils.isNotBlank(logRecord.et) &&
              StringUtils.isNotBlank(logRecord.dr) &&
              StrUtils.isInt(logRecord.dr) &&
              StringUtils.isNotBlank(logRecord.pp) &&
              StringUtils.isNotBlank(logRecord.scene) && //用scene替换 position_id
              StringUtils.isNotBlank(logRecord.wsr_query_ald_link_key) &&
              StringUtils.isNotBlank(logRecord.wsr_query_ald_media_id)
            ) {
              logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong, dateLong) //时间校正
              //把新用户的会话中的所有记录都标记为ifo=true
              markNewUser(logRecord, resource, baseRedisKey)

              val media_id = linkMap.get(logRecord.wsr_query_ald_link_key.trim)
              if (StringUtils.isNotBlank(media_id)) {
                logRecord.wsr_query_ald_media_id = media_id
                //用场景值替换，position_id
                if (logRecord.scene == "1058" ||
                  logRecord.scene == "1035" ||
                  logRecord.scene == "1014" ||
                  logRecord.scene == "1038") {
                  logRecord.wsr_query_ald_position_id = logRecord.scene
                } else {
                  logRecord.wsr_query_ald_position_id = "其它"
                }

                if (logRecord.v >= "7.0.0") {
                  if (logRecord.ev != "app") {
                    recordsRdd += logRecord
                  }
                } else {
                  recordsRdd += logRecord
                }
              }
            }
          })
        } catch {
          case jce: JedisConnectionException => jce.printStackTrace()
        } finally {
          if (resource != null) resource.close()
          if (redisCache != null) redisCache.close()
        }
        recordsRdd.iterator
      }).cache()

      val grap_map = cacheGrapUser("online_status_link") //缓存灰度用户上线状态
      val isOnLine = isOnline("online_status_link")
      LinkPatchAllTask.allStat_first(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
      LinkPatchAllTask.allStat_second(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
