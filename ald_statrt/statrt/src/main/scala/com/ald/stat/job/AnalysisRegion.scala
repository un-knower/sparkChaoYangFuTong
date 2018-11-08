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

/**
  * 地域分析 and 机型分析
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

    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    val stream = SQLKafkaConsume.getStream(ssc, group, topic)

    //关联手机品牌
    val newUserRedisKey = "rt:newUser"

    /** 把新用户的at缓存起来 */
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        use(redisCache.getResource) {
          resource =>
            par.foreach(line => {
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null && logRecord.ifo == "true") {
                resource.hset(newUserRedisKey, logRecord.at, "true")
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
                  //添加 手机型号，手机品牌信息
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
                    StringUtils.isNotBlank(logRecord.province)
                  ) {
                    //把新用户的会话中的所有记录都标记为ifo=true
                    val ifo = resource.hget(newUserRedisKey, logRecord.at.trim)
                    if (StringUtils.isNotBlank(ifo)) {
                      logRecord.ifo = ifo
                    }
                    recordsRdd += logRecord
                  }
                })
            }
            recordsRdd.iterator
          }).cache()

        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        //城市
        DailyCityTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
        //省份
        DailyProvinceTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

      })
    ssc.start()
    ssc.awaitTermination()
  }

}
