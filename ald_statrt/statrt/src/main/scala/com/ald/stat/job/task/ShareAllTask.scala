package com.ald.stat.job.task

import com.ald.stat.component.dimension.share.pageShare.{DailyPageShareSessionSubDimensionKey, DailyPageShareUidSubDimensionKey}
import com.ald.stat.component.dimension.share.share._
import com.ald.stat.component.dimension.share.userShare.{DailyUserShareSessionSubDimensionKey, DailyUserShareUidSubDimensionKey}
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.{SessionBaseImpl, SessionStatImpl, ShareDailySessionStat, ShareHourlySessionStat}
import com.ald.stat.module.uv.{ShareDailyUVStat, ShareHourlyUVStat}
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 分享概况任务
  * Created by admin on 2018/6/4.
  */
object ShareAllTask extends TaskTrait {

  /**
    * 分享概况  每日统计
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecord_click_rdd
    * @param logRecord_status_rdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def shareDailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecord_click_rdd: RDD[LogRecord], logRecord_status_rdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecord_click_rdd, DailyShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = ShareDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyShareSessionSubDimensionKey, redisPrefix)

    //分享人数  and 分享次数
    val share_pv_uv_rdd = ShareDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyShareUidSubDimensionKey, logRecord_status_rdd, redisPrefix)
    //分享被打开次数(回流量)  and  分享给打开人数
    val back_uv_op_rdd = ShareDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyShareSessionSubDimensionKey, logRecord_click_rdd, redisPrefix)
    //最终结果集
    val finalRDD = share_pv_uv_rdd.fullOuterJoin((back_uv_op_rdd).join(sessionRDD))

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)

        val share_new_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._2.newUserCount
        val share_user_count = row._2._1.getOrElse((0l, 0l))._2 //分享人数
        val share_count = row._2._1.getOrElse((0l, 0l))._1 //分享次数
        val share_open_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._1 //回流量
        val share_open_user_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._2 //分享被打开人数

        //分享回流比
        var share_reflux_ratio = 0f
        if (share_count != 0) {
          share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_share_summary
             |(
             |app_key,
             |day,
             |share_user_count,
             |new_count,
             |share_count,
             |share_open_count,
             |share_open_user_count,
             |share_reflux_ratio,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$share_user_count",
             |"$share_new_count",
             |"$share_count",
             |"$share_open_count",
             |"$share_open_user_count",
             |"$share_reflux_ratio",
             |now()
             |)
             |ON DUPLICATE KEY UPDATE
             |share_user_count="$share_user_count",
             |new_count="$share_new_count",
             |share_count="$share_count",
             |share_open_count="$share_open_count",
             |share_open_user_count="$share_open_user_count",
             |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
             |update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 分享概况  分时统计
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecord_click_rdd
    * @param logRecord_status_rdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def shareHourStat(baseRedisKey: String, taskId: String, dateStr: String, logRecord_click_rdd: RDD[LogRecord], logRecord_status_rdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecord_click_rdd, HourShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = ShareHourlySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourShareSessionSubDimensionKey, redisPrefix)

    //分享人数  and 分享次数
    val share_pv_uv_rdd = ShareHourlyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourShareUidSubDimensionKey, logRecord_status_rdd, redisPrefix)
    //分享被打开次数(回流量)  and  分享给打开人数
    val back_uv_op_rdd = ShareHourlyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, HourShareSessionSubDimensionKey, logRecord_click_rdd, redisPrefix)
    //最终结果
    val finalRDD = share_pv_uv_rdd.fullOuterJoin((back_uv_op_rdd).join(sessionRDD))

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)

        val share_new_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._2.newUserCount
        val share_user_count = row._2._1.getOrElse((0l, 0l))._2 //分享人数
        val share_count = row._2._1.getOrElse((0l, 0l))._1 //分享次数
        val share_open_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._1 //回流量
        val share_open_user_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._2 //分享被打开人数

        //分享回流比
        var share_reflux_ratio = 0f
        if (share_count != 0) {
          share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_hourly_share_summary
             |(
             |app_key,
             |day,
             |hour,
             |share_user_count,
             |new_count,
             |share_count,
             |share_open_count,
             |share_open_user_count,
             |share_reflux_ratio,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$share_user_count",
             |"$share_new_count",
             |"$share_count",
             |"$share_open_count",
             |"$share_open_user_count",
             |"$share_reflux_ratio",
             |now()
             |)
             |ON DUPLICATE KEY UPDATE
             |share_user_count="$share_user_count",
             |new_count="$share_new_count",
             |share_count="$share_count",
             |share_open_count="$share_open_count",
             |share_open_user_count="$share_open_user_count",
             |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
             |update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 页面分享  每日统计
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecord_click_rdd
    * @param logRecord_status_rdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def pageShareDailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecord_click_rdd: RDD[LogRecord], logRecord_status_rdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecord_click_rdd, DailyPageShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = ShareDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyPageShareSessionSubDimensionKey, redisPrefix)

    //分享人数  and 分享次数
    val share_pv_uv_rdd = ShareDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyPageShareUidSubDimensionKey, logRecord_status_rdd, redisPrefix)
    //分享被打开次数(回流量)  and  分享给打开人数
    val back_uv_op_rdd = ShareDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyPageShareSessionSubDimensionKey, logRecord_click_rdd, redisPrefix)
    //最终结果
    val finalRDD = share_pv_uv_rdd.fullOuterJoin((back_uv_op_rdd).join(sessionRDD))

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val path = splits(2)

        val share_new_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._2.newUserCount
        val share_user_count = row._2._1.getOrElse((0l, 0l))._2 //分享人数
        val share_count = row._2._1.getOrElse((0l, 0l))._1 //分享次数
        val share_open_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._1 //回流量
        val share_open_user_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._2 //分享被打开人数

        //分享回流比
        var share_reflux_ratio = 0f
        if (share_count != 0) {
          share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_dailyshare_page
             |(
             |app_key,
             |day,
             |page_uri,
             |share_user_count,
             |new_count,
             |share_count,
             |share_open_count,
             |share_open_user_count,
             |share_reflux_ratio,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$path",
             |"$share_user_count",
             |"$share_new_count",
             |"$share_count",
             |"$share_open_count",
             |"$share_open_user_count",
             |"$share_reflux_ratio",
             |now()
             |)
             |ON DUPLICATE KEY UPDATE
             |share_user_count="$share_user_count",
             |new_count="$share_new_count",
             |share_count="$share_count",
             |share_open_count="$share_open_count",
             |share_open_user_count="$share_open_user_count",
             |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
             |update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 用户分享  每日统计
    *
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param records_rdd_userShare_click
    * @param records_rdd_userShare_status
    * @param kafkaProducer
    * @param redisPrefix
    */
  def userShareDailyStat(baseRedisKey: String, taskId: String, dateStr: String, records_rdd_userShare_click: RDD[LogRecord], records_rdd_userShare_status: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(records_rdd_userShare_click, DailyUserShareSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = ShareDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyUserShareSessionSubDimensionKey, redisPrefix)

    //分享人数  and 分享次数
    val share_pv_uv_rdd = ShareDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyUserShareUidSubDimensionKey, records_rdd_userShare_status, redisPrefix)
    //分享被打开次数(回流量)  and  分享给打开人数
    val back_uv_op_rdd = ShareDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyUserShareSessionSubDimensionKey, records_rdd_userShare_click, redisPrefix)
    //最终结果
    val finalRDD = share_pv_uv_rdd.fullOuterJoin((back_uv_op_rdd).join(sessionRDD))

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val share_uuid = splits(2)

        val share_new_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._2.newUserCount
        val share_user_count = row._2._1.getOrElse((0l, 0l))._2 //分享人数
        val share_count = row._2._1.getOrElse((0l, 0l))._1 //分享次数
        val share_open_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._1 //回流量
        val share_open_user_count = row._2._2.getOrElse(((0l, 0l), new SessionSum))._1._2 //分享被打开人数

        //分享回流比
        var share_reflux_ratio = 0f
        if (share_count != 0) {
          share_reflux_ratio = share_open_count.toFloat / share_count.toFloat
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_dailyshare_user
             |(
             |app_key,
             |day,
             |sharer_uuid,
             |share_user_count,
             |new_count,
             |share_count,
             |share_open_count,
             |share_open_user_count,
             |share_reflux_ratio,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$share_uuid",
             |"$share_user_count",
             |"$share_new_count",
             |"$share_count",
             |"$share_open_count",
             |"$share_open_user_count",
             |"$share_reflux_ratio",
             |now()
             |)
             |ON DUPLICATE KEY UPDATE
             |share_user_count="$share_user_count",
             |new_count="$share_new_count",
             |share_count="$share_count",
             |share_open_count="$share_open_count",
             |share_open_user_count="$share_open_user_count",
             |share_reflux_ratio=ifnull(round(share_open_count/share_count,2),0),
             |update_at = now()
          """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

}
