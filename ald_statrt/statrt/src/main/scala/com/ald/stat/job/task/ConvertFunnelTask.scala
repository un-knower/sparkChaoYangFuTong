package com.ald.stat.job.task


import com.ald.stat.component.dimension.event.eventAnalyse.{DailyAtSubDimensionKey, DailyUuSubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.at.{ATStat, UUStat}
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.ald.stat.job.task.EventTask.writeKafkaEvent

/**
  * Created by spark01 on 6/8/18.
  */
object ConvertFunnelTask extends TaskTrait{

  def dailyStat(sparkSession: SparkSession, baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    //gcs:转换漏斗的模块和事件分析的时间参数用的表是一样的。所以转换漏斗使用时间分析的代码就可以了
    /** ------------------------------convertFunnel----------------------------- */
    //gcs:uu的数目的统计
    val uuCountRDD_eventAnalyse = UUStat.statIncreaseCache(baseRedisKey, dateStr,DailyUuSubDimensionKey, logRecordRdd, redisPrefix)
    val atCountRdd_eventAnalyse =ATStat.statIncreaseCache(baseRedisKey, dateStr,DailyAtSubDimensionKey, logRecordRdd, redisPrefix)

    //gcs:最后的结果的类型：RDD[(DimensionKey, ((Long, String), (Long, String)))]
    val finalRDD_eventAnalyse = uuCountRDD_eventAnalyse.join(atCountRdd_eventAnalyse) //gcs:uuCount在前，atCount在后
    writeKafkaEvent(kafkaProducer,finalRDD_eventAnalyse)
  }
}
