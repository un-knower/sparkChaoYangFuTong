package com.ald.stat.job.task

import com.ald.stat.component.dimension.link.link._
import com.ald.stat.component.dimension.link.media._
import com.ald.stat.component.dimension.link.position._
import com.ald.stat.component.dimension.link.summary.{LinkSessionSubDimensionKey, LinkUidSubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.{LinkDailySessionStat, LinkHourlySessionStat, SessionBaseImpl}
import com.ald.stat.module.uv.{LinkDailyUVStat, LinkHourlyUVStat}
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 外链分析
  * 包括：外链分析 媒体分析  位置分析
  * Created by admin on 2018/5/27.
  */
object LinkAllTask extends TaskTrait {

  def allStat_first(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    summaryStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

    dailyLinkStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

    hourLinkStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
  }

  def allStat_second(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    dailyMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

    hourMediaStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

    dailyPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)

    hourPositionStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
  }

  /**
    * 外链分析 头部汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def summaryStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = LinkDailySessionStat.stat(logRecordRdd, LinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, LinkSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = LinkDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, LinkUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = LinkDailyUVStat.statIncreaseCache(baseRedisKey, dateStr, LinkSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1

        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_link_summary
             |(
             |app_key,
             |day,
             |link_visitor_count,
             |link_open_count,
             |link_page_count,
             |link_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$visitor_count",
             | "$open_count",
             | "$total_page_count",
             | "$new_comer_count",
             | "$total_stay_time",
             | "$secondary_avg_stay_time",
             | "$one_page_count",
             | "$bounce_rate",
             | now()
             | )
             | ON DUPLICATE KEY UPDATE
             | link_visitor_count="$visitor_count",
             | link_open_count="$open_count",
             | link_page_count="$total_page_count",
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 外链分析 每日汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyLinkStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = LinkDailySessionStat.stat(logRecordRdd, DailyLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyLinkSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = LinkDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyLinkUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = LinkDailyUVStat.statIncreaseCache(baseRedisKey, dateStr, DailyLinkSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val link_id = splits(2)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1

        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
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


        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_link
             |(
             |app_key,
             |day,
             |link_key,
             |link_visitor_count,
             |link_open_count,
             |link_page_count,
             |link_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$link_id",
             | "$visitor_count",
             | "$open_count",
             | "$total_page_count",
             | "$new_comer_count",
             | "$total_stay_time",
             | "$secondary_avg_stay_time",
             | "$one_page_count",
             | "$bounce_rate",
             | now()
             | )
             | ON DUPLICATE KEY UPDATE
             | link_visitor_count="$visitor_count",
             | link_open_count="$open_count",
             | link_page_count="$total_page_count",
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 外链分析 每日详情
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourLinkStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = LinkHourlySessionStat.stat(logRecordRdd, HourLinkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkHourlySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourLinkSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = LinkHourlyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourLinkUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = LinkHourlyUVStat.statIncreaseCache(baseRedisKey, dateStr, HourLinkSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val link_id = splits(3)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1


        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_link
             |(
             |app_key,
             |day,
             |hour,
             |link_key,
             |link_visitor_count,
             |link_open_count,
             |link_page_count,
             |link_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             | values
             | (
             | "$app_key",
             | "$day",
             | "$hour",
             | "$link_id",
             | "$visitor_count",
             | "$open_count",
             | "$total_page_count",
             | "$new_comer_count",
             | "$total_stay_time",
             | "$secondary_avg_stay_time",
             | "$one_page_count",
             | "$bounce_rate",
             | now()
             | )
             | ON DUPLICATE KEY UPDATE
             | link_visitor_count="$visitor_count",
             | link_open_count="$open_count",
             | link_page_count="$total_page_count",
             | link_newer_for_app="$new_comer_count",
             | total_stay_time="$total_stay_time",
             | secondary_stay_time=ifnull(round(total_stay_time/link_open_count,2),0),
             | one_page_count="$one_page_count",
             | bounce_rate=ifnull(round(one_page_count/link_open_count,2),0),
             | update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 媒体分析 每日汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = LinkDailySessionStat.stat(logRecordRdd, DailyMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = LinkDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyMediaUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = LinkDailyUVStat.statIncreaseCache(baseRedisKey, dateStr, DailyMediaSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val media_id = splits(2)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1

        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_media
             |(
             |app_key,
             |day,
             |media_id,
             |media_visitor_count,
             |media_open_count,
             |media_page_count,
             |media_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$media_id",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |media_visitor_count="$visitor_count",
             |media_open_count="$open_count",
             |media_page_count="$total_page_count",
             |media_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/media_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/media_open_count,2),0),
             |update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 媒体分析 每日详情
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourMediaStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = LinkHourlySessionStat.stat(logRecordRdd, HourMediaSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkHourlySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourMediaSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = LinkHourlyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourMediaUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = LinkHourlyUVStat.statIncreaseCache(baseRedisKey, dateStr, HourMediaSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val media_id = splits(3)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1

        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_media
             |(
             |app_key,
             |day,
             |hour,
             |media_id,
             |media_visitor_count,
             |media_open_count,
             |media_page_count,
             |media_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$media_id",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |media_visitor_count="$visitor_count",
             |media_open_count="$open_count",
             |media_page_count="$total_page_count",
             |media_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/media_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/media_open_count,2),0),
             |update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })

  }

  /**
    * 位置分析 每日汇总
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def dailyPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = LinkDailySessionStat.stat(logRecordRdd, DailyPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = LinkDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = LinkDailyUVStat.statIncreaseCache(baseRedisKey, dateStr, DailyPositionSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val position_id = splits(2)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1

        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_position
             |(
             |app_key,
             |day,
             |position_id,
             |position_visitor_count,
             |position_open_count,
             |position_page_count,
             |position_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$position_id",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |position_visitor_count="$visitor_count",
             |position_open_count="$open_count",
             |position_page_count="$total_page_count",
             |position_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/position_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/position_open_count,2),0),
             |update_at=now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })

  }

  /**
    * 位置分析 每日详情
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    */
  def hourPositionStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = LinkHourlySessionStat.stat(logRecordRdd, HourPositionSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = LinkHourlySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourPositionSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = LinkHourlyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourPositionUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD = LinkHourlyUVStat.statIncreaseCache(baseRedisKey, dateStr, HourPositionSessionSubDimensionKey, logRecordRdd, redisPrefix)

    //最终结果集
    val finalRDD = uvRDD.join(opRDD).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val position_id = splits(3)

        val sessionSum = row._2._2
        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1
        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_position
             |(
             |app_key,
             |day,
             |hour,
             |position_id,
             |position_visitor_count,
             |position_open_count,
             |position_page_count,
             |position_newer_for_app,
             |total_stay_time,
             |secondary_stay_time,
             |one_page_count,
             |bounce_rate,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$position_id",
             |"$visitor_count",
             |"$open_count",
             |"$total_page_count",
             |"$new_comer_count",
             |"$total_stay_time",
             |"$secondary_avg_stay_time",
             |"$one_page_count",
             |"$bounce_rate",
             | now()
             |)
             |on duplicate key update
             |position_visitor_count="$visitor_count",
             |position_open_count="$open_count",
             |position_page_count="$total_page_count",
             |position_newer_for_app="$new_comer_count",
             |total_stay_time="$total_stay_time",
             |secondary_stay_time=ifnull(round(total_stay_time/position_open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/position_open_count,2),0),
             |update_at=now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

}
