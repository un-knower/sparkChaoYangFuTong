package com.ald.stat.job.patch.patchTask

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.trend._
import com.ald.stat.job.task.TaskTrait
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.{SessionBaseImpl, TrendDailySessionStat, TrendHourlySessionStat}
import com.ald.stat.module.uv.{TrendDailyUVStat, TrendHourlyUVStat}
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by zhaofw on 2018/8/16.
  */
object TrendPatchAllTask extends TaskTrait{

  /**
    * 趋势分析  每日统计 pv,uv,session open count
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    */
  def dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = TrendDailySessionStat.stat(logRecordRdd, DailySessionSubDimensionKey, SessionBaseImpl)
    val sessionRDD = TrendDailySessionStat.statPathSession(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailySessionSubDimensionKey, redisPrefix)
    val pvRDD = TrendDailyUVStat.statPatchPV(baseRedisKey, dateStr, DimensionKey, logRecordRdd, redisPrefix)
    val uvRDD = TrendDailyUVStat.statPatchUV(baseRedisKey, dateStr, DailyUidSubDimensionKey, logRecordRdd, redisPrefix)
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      par.foreach(row => {
        val kafka: KafkaSink[String, String] = kafkaProducer.value
        val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1

        var avg_stay_time = 0f
        if (open_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / open_count
        }
        var secondary_avg_stay_time = 0f
        if (total_page_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
        val updateAt = System.currentTimeMillis() / 1000

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_trend_analysis
             |(
             |    app_key,
             |    day,
             |    new_comer_count,
             |    visitor_count,
             |    open_count,
             |    total_page_count,
             |    avg_stay_time,
             |    secondary_avg_stay_time,
             |    total_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$total_page_count",
             |    "$avg_stay_time",
             |    "$secondary_avg_stay_time",
             |    "$total_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |new_comer_count="$new_comer_count",
             |visitor_count="$visitor_count",
             |open_count="$open_count",
             |total_stay_time="$total_stay_time",
             |total_page_count="$total_page_count",
             |avg_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |secondary_avg_stay_time=ifnull(round(total_stay_time/total_page_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
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
    * 趋势分析  分时统计 pv,uv,session open count
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    */
  def hourlyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                 kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                 grey_map:util.HashMap[String, String],isOnLine:Boolean,redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = TrendHourlySessionStat.stat(logRecordRdd, HourSessionSubDimensionKey, SessionBaseImpl)
    val sessionRDD = TrendHourlySessionStat.statPathSession(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourSessionSubDimensionKey, redisPrefix)
    val pvRDD = TrendHourlyUVStat.statPatchPV(baseRedisKey, dateStr, HourDimensionKey, logRecordRdd, redisPrefix)
    val uvRDD = TrendHourlyUVStat.statPatchUV(baseRedisKey, dateStr, HourUidSubDimensionKey, logRecordRdd, redisPrefix)
    // final rdd
    val finalRDD = pvRDD.join(uvRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      val updateAt = Timestamp.valueOf(LocalDateTime.now())
      par.foreach(row => {
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)

        val sessionSum = row._2._2

        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._2
        val open_count = sessionSum.sessionCount
        val total_page_count = row._2._1._1

        var avg_stay_time = 0d
        if (open_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / open_count
          if (avg_stay_time == null)
            avg_stay_time = 0d
        }

        var secondary_avg_stay_time = 0d
        if (total_page_count != 0) {
          secondary_avg_stay_time = sessionSum.sessionDurationSum / total_page_count
        }
        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageofSession
        //跳出率
        var bounce_rate = 0d
        if (open_count != 0) {
          bounce_rate = one_page_count.toFloat / open_count.toFloat
        }
        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_trend_analysis
             |(
             |    app_key,
             |    day,
             |    hour,
             |    new_comer_count,
             |    visitor_count,
             |    open_count,
             |    total_page_count,
             |    avg_stay_time,
             |    secondary_avg_stay_time,
             |    avg_refresh_count,
             |    total_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$hour",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$total_page_count",
             |    "$avg_stay_time",
             |    "$secondary_avg_stay_time",
             |     0,
             |    "$total_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |hour="$hour",
             |new_comer_count="$new_comer_count",
             |visitor_count="$visitor_count",
             |open_count="$open_count",
             |total_stay_time="$total_stay_time",
             |total_page_count="$total_page_count",
             |avg_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |secondary_avg_stay_time=ifnull(round(total_stay_time/total_page_count,2),0),
             |avg_refresh_count=0,
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
                          """.stripMargin

        //数据进入kafka
        //sendToKafka(kafka, sqlInsertOrUpdate)
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
