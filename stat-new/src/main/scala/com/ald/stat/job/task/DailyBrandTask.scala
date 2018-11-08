package com.ald.stat.job.task

import java.util

import com.ald.stat.component.dimension.phoneModel.{DailyBrandSessionSubDimensionKey, DailyBrandUidSubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.session.PhoneModel.BrandDailySessionStat
import com.ald.stat.module.session.SessionBaseImpl
import com.ald.stat.module.uv.PhoneModel.BrandDailyUVStat
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 手机品牌分析
  * Created by admin on 2018/5/25.
  */
object DailyBrandTask extends TaskTrait {

  def dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord],
                kafkaProducer: Broadcast[KafkaSink[String, String]],grey_kafkaProducer: Broadcast[KafkaSink[String, String]],
                grey_map:util.HashMap[String, String],isOnLine:Boolean, redisPrefix:Broadcast[String]):  Unit = {
    //重置redis prefix
    val tempSessMergeResult = BrandDailySessionStat.stat(logRecordRdd, DailyBrandSessionSubDimensionKey, SessionBaseImpl)
    //session处理
    val sessionRDD = BrandDailySessionStat.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyBrandSessionSubDimensionKey, redisPrefix)
    //uv
    val uvRDD = BrandDailyUVStat.statIncreaseCacheWithPV(baseRedisKey, dateStr, DailyBrandUidSubDimensionKey, logRecordRdd, redisPrefix)
    // final rdd
    val finalRDD = uvRDD.join(sessionRDD)

    finalRDD.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      val grey_kafka: KafkaSink[String, String] = grey_kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val brand = splits(2)

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

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_phonebrand
             |(
             |    app_key,
             |    day,
             |    brand,
             |    new_user_count,
             |    visitor_count,
             |    open_count,
             |    page_count,
             |    secondary_avg_stay_time,
             |    total_stay_time,
             |    one_page_count,
             |    bounce_rate,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$brand",
             |    "$new_comer_count",
             |    "$visitor_count",
             |    "$open_count",
             |    "$total_page_count",
             |    "$secondary_avg_stay_time",
             |    "$total_stay_time",
             |    "$one_page_count",
             |    "$bounce_rate",
             |    now()
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |brand="$brand",
             |new_comer_count="$new_comer_count",
             |visitor_count="$visitor_count",
             |open_count="$open_count",
             |total_stay_time="$total_stay_time",
             |page_count="$total_page_count",
             |secondary_stay_time=ifnull(round(total_stay_time/open_count,2),0),
             |one_page_count="$one_page_count",
             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
             |update_at=now()
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
