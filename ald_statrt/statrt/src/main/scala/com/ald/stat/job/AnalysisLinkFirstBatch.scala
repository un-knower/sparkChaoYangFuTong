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

object AnalysisLinkFirstBatch {

  val prefix = "jobLinkFirst"
  lazy val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
  val baseRedisKey = "rt";

  /**
    *
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

    //init scene meta data to redis
    val linkRedisKey = "rt:link"
    val resource = redisCache.getResource
    if (!resource.exists(linkRedisKey)) {
      use(getConnection()) { conn =>
        use(conn.createStatement()) {
          statement =>
            val rs = statement.executeQuery(
              """
                |select link_key,media_id
                |from ald_link_trace
                |where is_del=0
              """.stripMargin)
            while (rs.next()) {
              val link_key = rs.getString(1)
              val media_id = rs.getInt(2)
              if (link_key != null && media_id != null)
                resource.hset(linkRedisKey, link_key.toString, link_key.toString) //缓存link_key
              resource.hset(linkRedisKey, link_key.toString, media_id.toString) //缓存media_id
            }
        }
      }
    }
    try {
      if (resource != null) resource.close()
    }
    catch {
      case t: Throwable => t.printStackTrace()
    }
    //广播
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    val stream = SQLKafkaConsume.getStream(ssc, group, topic)

    // TODO: 这个逻辑需要修改，应该把赋值操作放在executor端执行 
    /** 把新用户的at缓存起来 */
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        use(redisCache.getResource) {
          resource =>
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null && logRecord.ifo == "true") {
                resource.hset(linkRedisKey, logRecord.at, "true")
              }
            })
        }
      })
    })

    stream.repartition(partitionNumber)
      .foreachRDD(rdd => {
        val logRecordRdd = rdd.mapPartitions(
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
                    StringUtils.isNotBlank(logRecord.dr) &&
                    StringUtils.isNotBlank(logRecord.pp) &&
                    StringUtils.isNotBlank(logRecord.scene) && //用scene替换 position_id
                    StringUtils.isNotBlank(logRecord.ag_ald_link_key) &&
                    StringUtils.isNotBlank(logRecord.ag_ald_media_id)
                  ) {
                    //把新用户的会话中的所有记录都标记为ifo=true
                    val ifo = resource.hget(linkRedisKey, logRecord.at.trim)
                    if (StringUtils.isNotBlank(ifo)) {
                      logRecord.ifo = ifo
                    }

                    val media_id = resource.hget(linkRedisKey, logRecord.ag_ald_link_key.trim())
                    if (StringUtils.isNotBlank(media_id)) {
                      logRecord.ag_ald_media_id = media_id
                      //用场景值替换，position_id
                      if (logRecord.scene == "1058" ||
                        logRecord.scene == "1035" ||
                        logRecord.scene == "1014" ||
                        logRecord.scene == "1038") {
                        logRecord.ag_ald_position_id = logRecord.scene
                      } else {
                        logRecord.ag_ald_position_id = "其他"
                      }

                      recordsRdd += logRecord
                    }
                  }
                })
            }
            recordsRdd.iterator
          }).cache()

        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        LinkAllTask.allStat_first(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}
