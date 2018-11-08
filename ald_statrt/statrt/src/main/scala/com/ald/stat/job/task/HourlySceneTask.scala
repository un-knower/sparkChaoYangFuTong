package com.ald.stat.job.task

import java.sql.Timestamp
import java.time.LocalDateTime

import com.ald.stat.component.dimension.scene._
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.{SessionBaseImpl, SessionStatImpl}
import com.ald.stat.module.uv.UVStat
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

//场景分时
object HourlySceneTask extends TaskTrait {

  /**
    * 小时分析 pv,uv,session open count
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    */
  def hourlyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {


    val tempSessMergeResult = SessionStatImpl.stat(logRecordRdd, HourSceneSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourSceneSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = UVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourSceneUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = UVStat.statIncreaseCache(baseRedisKey, dateStr, HourSceneSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val scene_id = splits(4)

        val sessionSum = row._2._2

        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val page_count = row._2._1._1._1

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
             |new_comer_count=new_comer_count+"$new_comer_count",
             |visitor_count=visitor_count+"$visitor_count",
             |open_count=open_count+"$open_count",
             |total_stay_time=total_stay_time+"$total_stay_time",
             |page_count=page_count+"$page_count",
             |secondary_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |one_page_count=one_page_count+"$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin
        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }


  /**
    * 小时分析 pv,uv,session open count
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    */
  def hourlyGroupStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = SessionStatImpl.stat(logRecordRdd, HourSceneGroupSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourSceneGroupSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = UVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourSceneGroupUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = UVStat.statIncreaseCache(baseRedisKey, dateStr, HourSceneGroupSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val scene_group_id = splits(3)

        val sessionSum = row._2._2

        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val page_count = row._2._1._1._1

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
             |new_comer_count=new_comer_count+"$new_comer_count",
             |visitor_count=visitor_count+"$visitor_count",
             |open_count=open_count+"$open_count",
             |total_stay_time=total_stay_time+"$total_stay_time",
             |page_count=page_count+"$page_count",
             |secondary_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |one_page_count=one_page_count+"$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin
        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }


}
