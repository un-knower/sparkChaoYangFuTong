package com.ald.stat.job.patch.patchTask

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.scene._
import com.ald.stat.component.dimension.trend.HourDimensionKey
import com.ald.stat.job.task.TaskTrait
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.scene.{SceneDailySessionStat, SceneGroupDailySessionStat, SceneGroupHourSessionStat, SceneHourSessionStat}
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.uv.scene.{SceneDailyUVStat, SceneGroupDailyUVStat, SceneGroupHourUVStat, SceneHourUVStat}
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object ScenePatchAllTask extends TaskTrait{

  def hourlyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                 kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                 grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = SceneHourSessionStat.stat(logRecordRdd, HourSceneSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SceneHourSessionStat.statPathSession(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourSceneSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = SceneHourUVStat.statPatchPV(baseRedisKey, dateStr, HourSceneDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = SceneHourUVStat.statPatchUV(baseRedisKey, dateStr, HourSceneUidSubDimensionKey, logRecordRdd, redisPrefix)

    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val scene_id = splits(4)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._2
        val open_count = sessionSum.sessionCount
        val page_count = row._2._1._1

        var secondary_stay_time = 0f
        if (open_count != 0) {
          secondary_stay_time = sessionSum.sessionDurationSum / open_count
        }
        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
        val updateAt = Timestamp.valueOf(LocalDateTime.now())
        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_scene
             |(
             |    app_key,
             |    day,
             |    hour,
             |    scene_id,
             |    new_comer_count,
             |    visitor_count,
             |    open_count,
             |    page_count,
             |    total_stay_time,
             |    secondary_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$hour",
             |    "$scene_id",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$page_count",
             |    "$total_stay_time",
             |    "$secondary_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |hour="$hour",
             |day="$day",
             |scene_id="$scene_id",
             |new_comer_count="$new_comer_count",
             |visitor_count="$visitor_count",
             |open_count="$open_count",
             |total_stay_time="$total_stay_time",
             |page_count="$page_count",
             |secondary_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin

        //数据进入kafka
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }


  /**
    * 场景值组，小时分析
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    */
  def hourlyGroupStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                      kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                      grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = SceneGroupHourSessionStat.stat(logRecordRdd, HourSceneGroupSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SceneGroupHourSessionStat.statPathSession(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourSceneGroupSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = SceneGroupHourUVStat.statPatchPV(baseRedisKey, dateStr, HourSceneGroupDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = SceneGroupHourUVStat.statPatchUV(baseRedisKey, dateStr, HourSceneGroupUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val scene_group_id = splits(3)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._2
        val open_count = sessionSum.sessionCount
        val page_count = row._2._1._1

        var secondary_stay_time = 0f
        if (open_count != 0) {
          secondary_stay_time = sessionSum.sessionDurationSum / open_count
        }
        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
        val updateAt = Timestamp.valueOf(LocalDateTime.now())
        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_scene_group
             |(
             |    app_key,
             |    day,
             |    hour,
             |    scene_group_id,
             |    new_comer_count,
             |    visitor_count,
             |    open_count,
             |    page_count,
             |    total_stay_time,
             |    secondary_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$hour",
             |    "$scene_group_id",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$page_count",
             |    "$total_stay_time",
             |    "$secondary_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |hour="$hour",
             |day="$day",
             |scene_group_id="$scene_group_id",
             |new_comer_count="$new_comer_count",
             |visitor_count="$visitor_count",
             |open_count="$open_count",
             |total_stay_time="$total_stay_time",
             |page_count="$page_count",
             |secondary_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin
        //数据进入kafka
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

  def dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SceneDailySessionStat.stat(logRecordRdd, DailySceneSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SceneDailySessionStat.statPathSession(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailySceneSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = SceneDailyUVStat.statPatchPV(baseRedisKey, dateStr, DailySceneDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = SceneDailyUVStat.statPatchUV(baseRedisKey, dateStr, DailySceneUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD)
    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val sceneId = splits(3)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._2
        val open_count = sessionSum.sessionCount
        val page_count = row._2._1._1

        val total_stay_time = sessionSum.sessionDurationSum

        var secondary_stay_time = 0f
        if (open_count != 0) {
          secondary_stay_time = sessionSum.sessionDurationSum / open_count
        }
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
        val updateAt = System.currentTimeMillis() / 1000

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_scene_statistics
             |(
             |    app_key,
             |    day,
             |    scene_id,
             |    scene_newer_for_app,
             |    scene_visitor_count,
             |    scene_open_count,
             |    scene_page_count,
             |    secondary_stay_time,
             |    total_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$sceneId",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$page_count",
             |    "$secondary_stay_time",
             |    "$total_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |scene_id="$sceneId",
             |scene_newer_for_app="$new_comer_count",
             |scene_visitor_count="$visitor_count",
             |scene_open_count="$open_count",
             |scene_page_count="$page_count",
             |secondary_stay_time=ifnull(round(total_stay_time/scene_open_count,2),0),
             |total_stay_time="$total_stay_time",
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/scene_open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin
        //数据进入kafka
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }


  /**
    * 场景值组  每日分析
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def dailyGroupStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                     kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                     grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SceneGroupDailySessionStat.stat(logRecordRdd, DailySceneGroupSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SceneGroupDailySessionStat.statPathSession(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailySceneGroupSessionSubDimensionKey, redisPrefix)
    //uv
    val pvRDD = SceneGroupDailyUVStat.statPatchPV(baseRedisKey, dateStr, DailySceneGroupDimensionKey, logRecordRdd, redisPrefix)

    val uvRDD = SceneGroupDailyUVStat.statPatchUV(baseRedisKey, dateStr, DailySceneGroupUidSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val scene_group_id = splits(2)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._2
        val open_count = sessionSum.sessionCount
        val page_count = row._2._1._1

        val total_stay_time = sessionSum.sessionDurationSum
        var secondary_stay_time = 0f
        if (open_count != 0) {
          secondary_stay_time = sessionSum.sessionDurationSum / open_count
        }
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
        val updateAt = Timestamp.valueOf(LocalDateTime.now())
        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_scene_group
             |(
             |    app_key,
             |    day,
             |    scene_group_id,
             |    new_comer_count,
             |    visitor_count,
             |    open_count,
             |    page_count,
             |    total_stay_time,
             |    secondary_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$scene_group_id",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$page_count",
             |    "$total_stay_time",
             |    "$secondary_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |scene_group_id="$scene_group_id",
             |new_comer_count="$new_comer_count",
             |visitor_count="$visitor_count",
             |open_count="$open_count",
             |total_stay_time="$total_stay_time",
             |page_count="$page_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin
        //数据进入kafka
        if(isOnLine){
          sendToKafka(kafka, sqlInsertOrUpdate) //如果上线状态为1，直接入生产kafka
        }else{
          if (isGrey(grey_map,app_key)) {
            sendToKafka(kafka, sqlInsertOrUpdate)
          }else{
            sendToGreyKafka(grey_kafka, sqlInsertOrUpdate)
          }
        }
      })
    })
  }

}
