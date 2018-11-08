package com.ald.stat.job.task

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.dimension.link.link.DailyEntrancePageSessionSubDimensionKey
import com.ald.stat.log.LogRecord
import com.ald.stat.module.entrancePage.{EntrancePageSessionImpl, EntrancePageSessionStatImpl, EntrancePageSessionSum}
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

/**
  * @author mike
  * 入口页面任务
  */
object EntrancePageTask extends TaskTrait {

  def dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    val tempSessMergeResult = EntrancePageSessionStatImpl.stat(logRecordRdd, DailyEntrancePageSessionSubDimensionKey, EntrancePageSessionImpl)
    //session处理
    val sessionRDD = EntrancePageSessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult, DailyEntrancePageSessionSubDimensionKey, redisPrefix)
    //ak:day:pagePath 汇总
    writeKafkaByPath(kafkaProducer, sessionRDD)
    //ak:day 汇总
    writeKafkaByTotal(kafkaProducer, sessionRDD)
  }

  /**
    * 按照路径汇总
    *
    * @param kafkaProducer kafka
    * @param rdd rdd
    */
  def writeKafkaByPath(kafkaProducer: Broadcast[KafkaSink[String, String]], rdd: RDD[(DimensionKey, EntrancePageSessionSum)]) {

    println("entrance page write to kafka!")
    rdd.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        val pagePath = splits(2)

        val sessionSum = row._2
        val visitor_count = sessionSum.uv
        val open_count = sessionSum.openCount
        val page_count = sessionSum.pv

        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
        }
        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageOfSessionSum
        val entry_page_count = sessionSum.firstPageOfSessionSum
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count / open_count
        }
        val updateAt = System.currentTimeMillis() / 1000

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_entrance_page
             |(app_key,
             |day,
             |page_path,
             |entry_page_count,
             |one_page_count,
             |page_count,
             |visitor_count,
             |open_count,
             |total_time,
             |avg_stay_time,
             |bounce_rate,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$pagePath",
             |"$entry_page_count",
             |"$one_page_count",
             |"$page_count",
             |"$visitor_count",
             |"$open_count",
             |"$total_stay_time",
             |"$avg_stay_time",
             |"$bounce_rate",
             |"$updateAt"
             |)
             |ON DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |page_path="$pagePath",
             |entry_page_count=entry_page_count+"$entry_page_count",
             |one_page_count=one_page_count+"$one_page_count",
             |page_count=page_count+"$page_count",
             |visitor_count=visitor_count+"$visitor_count",
             |open_count=open_count+"$open_count",
             |total_time=total_time+"$total_stay_time",
             |avg_stay_time=avg_stay_time+"$avg_stay_time",
             |bounce_rate=bounce_rate+"$bounce_rate",
             |update_at="$updateAt"
            """.stripMargin
        println(sqlInsertOrUpdate)
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

  /**
    * 将数据按天聚合
    *
    * @param kafkaProducer kafka
    * @param rdd rdd
    */
  def writeKafkaByTotal(kafkaProducer: Broadcast[KafkaSink[String, String]], rdd: RDD[(DimensionKey, EntrancePageSessionSum)]): Unit = {

    val dayRdd = rdd.map(x => {
      (x._1.key.substring(0, x._1.key.lastIndexOf(":")), x._2)
    }).reduceByKey((x, y) => {
      EntrancePageSessionStatImpl.sumSessionSum(x, y)
    })
    writeKafkaByDay(kafkaProducer, dayRdd)
  }

  /**
    * 按天汇总
    *
    * @param kafkaProducer kafka producer
    * @param rdd           rdd
    */
  def writeKafkaByDay(kafkaProducer: Broadcast[KafkaSink[String, String]], rdd: RDD[(String, EntrancePageSessionSum)]) {

    rdd.foreachPartition(par => {
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)

        val sessionSum = row._2
        val visitor_count = sessionSum.uv
        val open_count = sessionSum.openCount
        val page_count = sessionSum.pv

        var avg_stay_time = 0f
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
        }
        val total_stay_time = sessionSum.sessionDurationSum
        val one_page_count = sessionSum.onePageOfSessionSum
        val entry_page_count = sessionSum.firstPageOfSessionSum
        //跳出率
        var bounce_rate = 0f
        if (open_count != 0) {
          bounce_rate = one_page_count / open_count
        }
        val updateAt = System.currentTimeMillis() / 1000

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_entrance_page
             |(app_key,
             |day,
             |entry_page_count,
             |one_page_count,
             |page_count,
             |visitor_count,
             |open_count,
             |total_time,
             |avg_stay_time,
             |bounce_rate,
             |update_at
             |)
             |values
             |(
             |"$app_key",
             |"$day",
             |"$entry_page_count",
             |"$one_page_count",
             |"$page_count",
             |"$visitor_count",
             |"$open_count",
             |"$total_stay_time",
             |"$avg_stay_time",
             |"$bounce_rate",
             |"$updateAt"
             |)
             |ON DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |entry_page_count=entry_page_count+"$entry_page_count",
             |one_page_count=one_page_count+"$one_page_count",
             |page_count=page_count+"$page_count",
             |visitor_count=visitor_count+"$visitor_count",
             |open_count=open_count+"$open_count",
             |total_time=total_time+"$total_stay_time",
             |avg_stay_time=avg_stay_time+"$avg_stay_time",
             |bounce_rate=bounce_rate+"$bounce_rate",
             |update_at="$updateAt"
            """.stripMargin
        sendToKafka(kafka, sqlInsertOrUpdate)
      })
    })
  }

}


