package com.ald.stat.job

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.patch.patchTask.TrendPatchAllTask
import com.ald.stat.job.task.{DailyAnalysisTrendTask, HourlyAnalysisTrendTask}
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

//gcs:从这个类rru kou qu kan dai ma jiu ke yi le
object AnalysisTrend extends AbstractBaseJob {

  val prefix = "default"
  val offsetPrefix = "trend_offset"
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
      //                  .set("spark.executor.cores", "2")
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


//    val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
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

    /**业务操作*/
    stream.repartition(partitionNumber).foreachRDD(rdd => {
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
        val resource = redisCache.getResource

        val recordsRdd = ArrayBuffer[LogRecord]()
        try {
          par.foreach(line => {
            //RddUtils.checkAndSaveOffset(dateStr, line.topic(), group, line, resource_offset)
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.at) &&
              StringUtils.isNotBlank(logRecord.ev) &&
              StringUtils.isNotBlank(logRecord.uu) &&
              StringUtils.isNotBlank(logRecord.et) &&
              StringUtils.isNotBlank(logRecord.dr) &&
              StringUtils.isNotBlank(logRecord.pp)&&
              StrUtils.isInt(logRecord.dr)) {
              logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
              if (logRecord.v != null && logRecord.v < "7.0.0") {
                markNewUser(logRecord, resource, baseRedisKey)
              }
              //如果是7.0.0以上的版本，则去除掉ev=app的数据，避免多算新用户数
              if(logRecord.v >= "7.0.0"){
                if(logRecord.ev != "app"){
                  recordsRdd += logRecord
                }
              }else{
                recordsRdd += logRecord
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

      val grap_map = cacheGrapUser("online_status") //缓存灰度用户上线状态
      val isOnLine = isOnline("online_status")
      //趋势分析
      HourlyAnalysisTrendTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
      DailyAnalysisTrendTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
