package com.ald.stat.job.patch.patchJob

import java.util
import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AbstractBaseJob
import com.ald.stat.job.patch.AnalysisScenePatch.{cacheGrapUser, handleNewUserForOldSDK, isOnline, markNewUser}
import com.ald.stat.job.patch.patchTask.ScenePatchAllTask
import com.ald.stat.job.task.{DailySceneTask, HourlySceneTask}
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object AnalysisScenePatch extends AbstractBaseJob {

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


        val stream_rdd = KafkaConsume.rddFromOffsetRangeToPatch(sparkContext, topic, group, baseRedisKey, offsetPrefix)
        handleNewUserForOldSDK(stream_rdd, baseRedisKey, prefix)
        //其他的分组ID
        val otherSceneId = ConfigUtils.getProperty("other.scene.id")
        val otherSceneGroupId = ConfigUtils.getProperty("other.scene.group.id")
        val unknownSceneGroupId = ConfigUtils.getProperty("unknown.scene.id")
        val unknownSceneId = ConfigUtils.getProperty("unknown.scene.group.id")

        val logRecordRdd: RDD[LogRecord] = stream_rdd.repartition(partitionNumber).mapPartitions(par => {
          val dateStr = ComputeTimeUtils.getDateStr(new Date())
          val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
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
                StringUtils.isNotBlank(logRecord.dr) &&
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

        //趋势分析
        val grap_map = cacheGrapUser("online_status_scene") //缓存灰度用户上线状态
        val isOnLine = isOnline("online_status_scene")

        //趋势分析
    ScenePatchAllTask.hourlyGroupStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)  //场景值组，分时统计
    ScenePatchAllTask.dailyGroupStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix)  //场景值组，每日统计
    ScenePatchAllTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix) //场景值，分时统计
    ScenePatchAllTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer,grey_kafkaProducer, grap_map, isOnLine, redisPrefix) //场景值，每日统计
  }

}