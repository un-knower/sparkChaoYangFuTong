package com.ald.stat.job.task

import java.util

import com.ald.stat.component.dimension.regional.{DailyProvinceSessionSubDimensionKey, DailyProvinceUidSubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.region.ProvinceDailySessionStat
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.uv.region.ProvinceDailyUVStat
import com.ald.stat.utils.{ComputeTimeUtils, KafkaSink}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 地域分析——省份分析
  * Created by admin on 2018/5/25.
  */
object DailyProvinceTask extends TaskTrait {
  def dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix: Broadcast[String]): Unit = {
    val tempSessMergeResult = ProvinceDailySessionStat.stat(logRecordRdd, DailyProvinceSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = ProvinceDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyProvinceSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = ProvinceDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyProvinceUidSubDimensionKey, logRecordRdd, redisPrefix)
    //最终结果集
    val finalRDD = uvRDD.join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = ComputeTimeUtils.tranTimeToLong(splits(1))
        //yyyyMMdd转时间戳
        val province = splits(2)

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
             |insert into aldstat_region_statistics
             |(
             |    app_key,
             |    day,
             |    province,
             |    new_user_count,
             |    visitor_count,
             |    open_count,
             |    page_count,
             |    secondary_stay_time,
             |    total_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$province",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$total_page_count",
             |    "$secondary_avg_stay_time",
             |    "$total_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |province="$province",
             |new_user_count="$new_comer_count",
             |visitor_count="$visitor_count",
             |open_count="$open_count",
             |total_stay_time="$total_stay_time",
             |page_count="$total_page_count",
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
