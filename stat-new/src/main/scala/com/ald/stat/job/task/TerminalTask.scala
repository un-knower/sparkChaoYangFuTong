package com.ald.stat.job.task

import java.sql.Timestamp
import java.time.LocalDateTime

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.terminal.lang.{DailyLangDimensionKey, DailyLangSessionSubDimensionKey, DailyLangUidSubDimensionKey}
import com.ald.stat.component.dimension.terminal.nt.{DailyNtDimensionKey, DailyNtSessionSubDimensionKey, DailyNtUidSubDimensionKey}
import com.ald.stat.component.dimension.terminal.sv.{DailySvDimensionKey, DailySvSessionSubDimensionKey, DailySvUidSubDimensionKey}
import com.ald.stat.component.dimension.terminal.wsdk.{DailyWsdkDimensionKey, DailyWsdkSessionSubDimensionKey, DailyWsdkUidSubDimensionKey}
import com.ald.stat.component.dimension.terminal.wv.{DailyWvDimensionKey, DailyWvSessionSubDimensionKey, DailyWvUidSubDimensionKey}
import com.ald.stat.component.dimension.terminal.wvv.{DailyWvvDimensionKey, DailyWvvSessionSubDimensionKey, DailyWvvUidSubDimensionKey}
import com.ald.stat.component.dimension.terminal.ww_wh.{DailyWw_WhDimensionKey, DailyWw_WhSessionSubDimensionKey, DailyWw_WhUidSubDimensionKey}
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.module.pv.PVStat
import com.ald.stat.module.session.{SessionBaseImpl, SessionStatImpl}
import com.ald.stat.module.uv.UVStat
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


/**
  * 终端分析
  */
object TerminalTask extends TaskTrait {

  def dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    /** ------------------------------lang----------------------------- */
    val tempSessMergeResult_lang = SessionStatImpl.stat(logRecordRdd, DailyLangSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD_lang = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_lang, DailyLangSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD_lang = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyLangUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD_lang = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyLangSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //pv
    val pvRDD_lang = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyLangDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD_lang = pvRDD_lang.join(uvRDD_lang).join(opRDD_lang).join(sessionRDD_lang)

    writeKafka("lang", kafkaProducer, finalRDD_lang)
    /** -------------------------------nt------------------------------ */
    val tempSessMergeResult_nt = SessionStatImpl.stat(logRecordRdd, DailyNtSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD_nt = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_nt, DailyNtSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD_nt = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyNtUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD_nt = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyNtSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //pv
    val pvRDD_nt = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyNtDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD_nt = pvRDD_nt.join(uvRDD_nt).join(opRDD_nt).join(sessionRDD_nt)

    writeKafka("nt", kafkaProducer, finalRDD_nt)
    /** -------------------------------sv------------------------------ */
    val tempSessMergeResult_sv = SessionStatImpl.stat(logRecordRdd, DailySvSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD_sv = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_sv, DailySvSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD_sv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailySvUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD_sv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailySvSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //pv
    val pvRDD_sv = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailySvDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD_sv = pvRDD_sv.join(uvRDD_sv).join(opRDD_sv).join(sessionRDD_sv)

    writeKafka("sv", kafkaProducer, finalRDD_sv)
    /** -------------------------------wsdk------------------------------ */
    val tempSessMergeResult_wsdk = SessionStatImpl.stat(logRecordRdd, DailyWsdkSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD_wsdk = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_wsdk, DailyWsdkSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD_wsdk = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWsdkUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD_wsdk = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWsdkSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //pv
    val pvRDD_wsdk = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyWsdkDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD_wsdk = pvRDD_wsdk.join(uvRDD_wsdk).join(opRDD_wsdk).join(sessionRDD_wsdk)

    writeKafka("wsdk", kafkaProducer, finalRDD_wsdk)
    /** -------------------------------wv------------------------------ */
    val tempSessMergeResult_wv = SessionStatImpl.stat(logRecordRdd, DailyWvSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD_wv = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_wv, DailyWvSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD_wv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWvUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD_wv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWvSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //pv
    val pvRDD_wv = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyWvDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD_wv = pvRDD_wv.join(uvRDD_wv).join(opRDD_wv).join(sessionRDD_wv)

    writeKafka("wv", kafkaProducer, finalRDD_wv)
    /** -------------------------------wvv------------------------------ */
    val tempSessMergeResult_wvv = SessionStatImpl.stat(logRecordRdd, DailyWvvSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD_wvv = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_wvv, DailyWvvSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD_wvv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWvvUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD_wvv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWvvSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //pv
    val pvRDD_wvv = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyWvvDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD_wvv = pvRDD_wvv.join(uvRDD_wvv).join(opRDD_wvv).join(sessionRDD_wvv)

    writeKafka("wvv", kafkaProducer, finalRDD_wvv)
    /** -------------------------------ww_wh------------------------------ */
    val tempSessMergeResult_ww_wh = SessionStatImpl.stat(logRecordRdd, DailyWw_WhSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD_ww_wh = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_ww_wh, DailyWw_WhSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD_ww_wh = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWw_WhUidSubDimensionKey, logRecordRdd, redisPrefix)
    //open count
    val opRDD_ww_wh = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyWw_WhSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //pv
    val pvRDD_ww_wh = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyWw_WhDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD_ww_wh = pvRDD_ww_wh.join(uvRDD_ww_wh).join(opRDD_ww_wh).join(sessionRDD_ww_wh)

    writeKafka("ww_wh", kafkaProducer, finalRDD_ww_wh)

  }

  /**
    * 入kafka
    *
    * @param dimension_name
    * @param kafkaProducer
    * @param rdd
    */
  def writeKafka(dimension_name: String, kafkaProducer: Broadcast[KafkaSink[String, String]], rdd: RDD[(DimensionKey, (((Long, Long), Long), SessionSum))]) {
    rdd.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        var type_value = splits(2)
        //对像素比特殊处理下,把像素比的结构从ww_wh调整为ww*wh
        if (type_value.split("_").length > 1) {
          type_value = type_value.split("_")(0) + "*" + type_value.split("_")(1)
        }

        val sessionSum = row._2._2


        val new_comer_count = sessionSum.newUserCount
        val visitor_count = row._2._1._1._2
        val open_count = row._2._1._2
        val total_page_count = row._2._1._1._1
        var avg_stay_time = 0f
        if (open_count != 0) {          avg_stay_time = sessionSum.sessionDurationSum / open_count        }
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
             |insert into aldstat_terminal_analysis
             |(
             |    app_key,
             |    day,
             |    type,
             |    type_value,
             |    new_comer_count,
             |    visitor_count,
             |    open_count,
             |    total_page_count,
             |    avg_stay_time,
             |    total_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$dimension_name",
             |    "$type_value",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$total_page_count",
             |    "$avg_stay_time",
             |    "$total_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |type="$dimension_name",
             |type_value="$type_value",
             |new_comer_count=new_comer_count+"$new_comer_count",
             |visitor_count=visitor_count+"$visitor_count",
             |open_count=open_count+"$open_count",
             |total_stay_time=total_stay_time+"$total_stay_time",
             |total_page_count=total_page_count+"$total_page_count",
             |avg_stay_time=ifnull(round(total_stay_time/visitor_count,2),0),
             |one_page_count=one_page_count+"$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at="$updateAt"
            """.stripMargin

        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }
}


