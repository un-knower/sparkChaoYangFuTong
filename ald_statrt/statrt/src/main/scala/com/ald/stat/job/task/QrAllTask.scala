package com.ald.stat.job.task

import com.ald.stat.component.dimension.qrCode.qrCode.{DailyQrSessionSubDimensionKey, DailyQrUidSubDimensionKey, HourQrSessionSubDimensionKey, HourQrUidSubDimensionKey}
import com.ald.stat.component.dimension.qrCode.qrGroup.{DailyQrGroupSessionSubDimensionKey, DailyQrGroupUidSubDimensionKey, HourQrGroupSessionSubDimensionKey, HourQrGroupUidSubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.{SessionBaseImpl, SessionStatImpl}
import com.ald.stat.module.uv.UVStat
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 二维码统计任务
  * Created by admin on 2018/6/6.
  */
object QrAllTask extends TaskTrait{

  /**
    * 二维码统计  每日统计
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def qrDailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecordRdd, DailyQrSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyQrSessionSubDimensionKey, redisPrefix)
    //扫码总人数
    val scan_uv_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyQrUidSubDimensionKey, logRecordRdd, redisPrefix)
    //扫码总次数
    val scan_op_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyQrSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = scan_uv_rdd.join(scan_op_rdd).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val qr_key = splits(2)

        val sessionSum = row._2._2
        val scan_new_count = sessionSum.newUserCount  //扫码新增
        val total_scan_count = row._2._1._2 //扫码总次数
        val total_scan_user_count = row._2._1._1 //扫码总人数

        val updateAt = System.currentTimeMillis() / 1000

        val sqlInsertOrUpdate =
          s"""
             insert into aldstat_qr_code_statistics
             |(
             |app_key,
             |day,
             |qr_key,
             |total_scan_user_count,
             |total_scan_count,
             |qr_new_comer_for_app,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$qr_key",
             |"$total_scan_user_count",
             |"$total_scan_count",
             |"$scan_new_count",
             |"$updateAt"
             |)
             |ON DUPLICATE KEY UPDATE
             |total_scan_user_count=total_scan_user_count+$total_scan_user_count,
             |total_scan_count=total_scan_count+$total_scan_count,
             |qr_new_comer_for_app=qr_new_comer_for_app+$scan_new_count,
             |update_at=$updateAt
          """.stripMargin

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 二维码统计 分时统计
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def qrHourStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecordRdd, HourQrSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourQrSessionSubDimensionKey, redisPrefix)
    //扫码总人数
    val scan_uv_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, HourQrUidSubDimensionKey, logRecordRdd, redisPrefix)
    //扫码总次数
    val scan_op_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, HourQrSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = scan_uv_rdd.join(scan_op_rdd).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val qr_key = splits(3)

        val sessionSum = row._2._2
        val scan_new_count = sessionSum.newUserCount  //扫码新增
        val total_scan_count = row._2._1._2 //扫码总次数
        val total_scan_user_count = row._2._1._1 //扫码总人数

        val sqlInsertOrUpdate =
          s"""
             insert into aldstat_hourly_qr
             |(
             |app_key,
             |day,
             |hour,
             |qr_key,
             |qr_visitor_count,
             |qr_scan_count,
             |qr_newer_count,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$qr_key",
             |"$total_scan_count",
             |"$total_scan_user_count",
             |"$scan_new_count",
             | now()
             |)
             |ON DUPLICATE KEY UPDATE
             |qr_visitor_count=qr_visitor_count+$total_scan_count,
             |qr_scan_count=qr_scan_count+$total_scan_user_count,
             |qr_newer_count=qr_newer_count+$scan_new_count,
             |update_at=now()
          """.stripMargin

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 二维码组统计  每日统计
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def qrGroupDailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecordRdd, DailyQrGroupSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyQrGroupSessionSubDimensionKey, redisPrefix)
    //扫码总人数
    val scan_uv_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyQrGroupUidSubDimensionKey, logRecordRdd, redisPrefix)
    //扫码总次数
    val scan_op_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyQrGroupSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = scan_uv_rdd.join(scan_op_rdd).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val qr_group_key = splits(2)

        val sessionSum = row._2._2
        val scan_new_count = sessionSum.newUserCount  //扫码新增
        val total_scan_count = row._2._1._2 //扫码总次数
        val total_scan_user_count = row._2._1._1 //扫码总人数

        val sqlInsertOrUpdate =
          s"""
             insert into aldstat_daily_qr_group
             |(
             |app_key,
             |day,
             |qr_group_key,
             |qr_visitor_count,
             |qr_scan_count,
             |qr_newer_count,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$qr_group_key",
             |"$total_scan_count",
             |"$total_scan_user_count",
             |"$scan_new_count",
             | now()
             |)
             |ON DUPLICATE KEY UPDATE
             |qr_visitor_count=qr_visitor_count+$total_scan_count,
             |qr_scan_count=qr_scan_count+$total_scan_user_count,
             |qr_newer_count=$scan_new_count,
             |update_at=now()
          """.stripMargin

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 二维码组统计  分时统计
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param logRecordRdd
    * @param kafkaProducer
    * @param redisPrefix
    */
  def qrGroupHourStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = SessionStatImpl.stat(logRecordRdd, HourQrGroupSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, HourQrGroupSessionSubDimensionKey, redisPrefix)
    //扫码总人数
    val scan_uv_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, HourQrGroupUidSubDimensionKey, logRecordRdd, redisPrefix)
    //扫码总次数
    val scan_op_rdd = UVStat.statIncreaseCache(baseRedisKey, dateStr, HourQrGroupSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = scan_uv_rdd.join(scan_op_rdd).join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val hour = splits(2)
        val qr_group_key = splits(3)

        val sessionSum = row._2._2
        val scan_new_count = sessionSum.newUserCount  //扫码新增
        val total_scan_count = row._2._1._2 //扫码总次数
        val total_scan_user_count = row._2._1._1 //扫码总人数

        val sqlInsertOrUpdate =
          s"""
             insert into aldstat_hourly_qr_group
             |(
             |app_key,
             |day,
             |hour,
             |qr_group_key,
             |qr_visitor_count,
             |qr_scan_count,
             |qr_newer_count,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$hour",
             |"$qr_group_key",
             |"$total_scan_count",
             |"$total_scan_user_count",
             |"$scan_new_count",
             | now()
             |)
             |ON DUPLICATE KEY UPDATE
             |qr_visitor_count=qr_visitor_count+$total_scan_count,
             |qr_scan_count=qr_scan_count+$total_scan_user_count,
             |qr_newer_count=$scan_new_count,
             |update_at=now()
          """.stripMargin

        //数据进入kafka
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

}
