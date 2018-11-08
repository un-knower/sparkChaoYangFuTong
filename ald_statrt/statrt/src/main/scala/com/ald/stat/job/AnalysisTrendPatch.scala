package com.ald.stat.job

import java.util
import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.task.{DailyAnalysisTrendTask, HourlyAnalysisTrendTask}
import com.ald.stat.kafka.hbase.SQLKafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.DBUtils.{getConnection, use}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AnalysisTrendPatch {

  val prefix = "default"
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
    println("partitionNumber:" + partitionNumber)

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

    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    val stream = SQLKafkaConsume.getStream(ssc, group, topic)


    //其他的分组ID
    val otherSceneId = ConfigUtils.getProperty("other.scene.id")
    val otherSceneGroupId = ConfigUtils.getProperty("other.scene.group.id")
    val unknownSceneGroupId = ConfigUtils.getProperty("unknown.scene.id")
    val unknownSceneId = ConfigUtils.getProperty("unknown.scene.group.id")

    stream.repartition(partitionNumber).foreachRDD(rdd => {

      //init scene meta data to broadcast value
      val sceneMap = new util.HashMap[String, String]()
      use(getConnection()) { conn =>
        use(conn.createStatement()) {
          statement =>
            val rs = statement.executeQuery("select sid,scene_group_id from ald_cms_scene ")
            while (rs.next()) {
              val sid = rs.getInt(1)
              val sgid = rs.getInt(2)
              if (sid != null && sgid != null) {
                sceneMap.put(sid.toString, sgid.toString)
              }
            }
        }
      }
      //广播变量
      //      val broadcastSceneMap = ssc.sparkContext.broadcast(sceneMap)

      val logRecordRdd: RDD[LogRecord] = rdd.map(line => {
        LogRecord.line2Bean(line.value())
      }).filter(logRecord => {
        logRecord != null &&
          StringUtils.isNotBlank(logRecord.ak) &&
          StringUtils.isNotBlank(logRecord.at) &&
          StringUtils.isNotBlank(logRecord.ev) &&
          StringUtils.isNotBlank(logRecord.uu) &&
          StringUtils.isNotBlank(logRecord.et) &&
          StringUtils.isNotBlank(logRecord.dr) &&
          StringUtils.isNotBlank(logRecord.pp) &&
          StringUtils.isNotBlank(logRecord.scene)
      }).map(logRecord => {
        //未知
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
        logRecord
      }).cache()

      val dateStr = ComputeTimeUtils.getDateStr(new Date())
      //趋势分析
      HourlyAnalysisTrendTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
      DailyAnalysisTrendTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
      //      HourlySceneTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

    })
    ssc.start()
    ssc.awaitTermination()
  }

}
