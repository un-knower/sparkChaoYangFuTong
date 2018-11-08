package com.ald.stat.job

import java.util
import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.task.{DailySceneTask, HourlySceneTask}
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

object AnalysisScene extends AbstractBaseJob{

  val prefix = "jobScene"
  val offsetPrefix = "scene_offset"
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

    //val stream = KafkaConsume.getStream(ssc, group, topic) //用普通方式获取Dstram
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

    //其他的分组ID
    val otherSceneId = ConfigUtils.getProperty("other.scene.id")
    val otherSceneGroupId = ConfigUtils.getProperty("other.scene.group.id")
    val unknownSceneGroupId = ConfigUtils.getProperty("unknown.scene.id")
    val unknownSceneId = ConfigUtils.getProperty("unknown.scene.group.id")

    /**业务操作*/
    stream.repartition(partitionNumber).foreachRDD(rdd => {
      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
      val logRecordRdd = rdd.mapPartitions(par => {
        val sceneMap = new util.HashMap[String, String]()
        use(getConnection()) { conn =>
          use(conn.createStatement()) {
            statement =>
              val rs = statement.executeQuery(
                """
                  |select sid,scene_group_id
                  |from ald_cms_scene
                """.stripMargin)
              while (rs.next()) {
                val sid = rs.getString(1)
                val group_id = rs.getInt(2)
                if (sid != null && group_id != null) {
                  sceneMap.put(sid.toString, group_id.toString)
                }
              }
          }
        }

        val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
        val resource = redisCache.getResource
        val recordsRdd = ArrayBuffer[LogRecord]()
        try{
          par.foreach(line => {
            val logRecord = LogRecord.line2Bean(line.value())
            if (logRecord != null &&
              StringUtils.isNotBlank(logRecord.ak) &&
              StringUtils.isNotBlank(logRecord.at) &&
              StringUtils.isNotBlank(logRecord.ev) &&
              StringUtils.isNotBlank(logRecord.uu) &&
              StringUtils.isNotBlank(logRecord.et) &&
              StringUtils.isNotBlank(logRecord.dr) &&
              StrUtils.isInt(logRecord.dr)&&
              StringUtils.isNotBlank(logRecord.pp) &&
              StringUtils.isNotBlank(logRecord.scene)
            ) {

              logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正
              //把新用户的会话中的所有记录都标记为ifo=true
              markNewUser(logRecord, resource, baseRedisKey)

              if (StringUtils.isBlank(logRecord.scene)) {
                logRecord.scene = unknownSceneId
                logRecord.scene_group_id = unknownSceneGroupId
              } else {
                val sgid = sceneMap.get(logRecord.scene.trim)
                if (StringUtils.isNotBlank(sgid)) {
                  logRecord.scene_group_id = sgid
                } else {
                  //其他
                  logRecord.scene = otherSceneId
                  logRecord.scene_group_id = otherSceneGroupId
                }
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
        }catch {
          case jce: JedisConnectionException => jce.printStackTrace()
        } finally {
          if (resource != null) resource.close()
          if (redisCache != null) redisCache.close()
        }
        recordsRdd.iterator
      }).cache()

      val grap_map = cacheGrapUser("online_status_scene") //缓存灰度用户上线状态
      val isOnLine = isOnline("online_status_scene")
      HourlySceneTask.hourlyGroupStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)  //场景值组，分时统计
      DailySceneTask.dailyGroupStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)  //场景值组，每日统计
      HourlySceneTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix) //场景值，分时统计
      DailySceneTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix) //场景值，每日统计
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
