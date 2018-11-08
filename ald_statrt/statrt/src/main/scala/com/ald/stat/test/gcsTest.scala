package com.ald.stat.test

import java.util.{Date, Properties}

import com.ald.stat.component.dimension.depthAndDuration.{DailyUVDepthATSubDimensionKeyExtend, DailyUVDepthUUSubDimensionKeyExtend, DailyUVDurationATSubDimensionKeyExtend, DailyUVDurationUUSubSimensionKeyExtend}
import com.ald.stat.kafka.hbase.SQLKafkaConsume
import com.ald.stat.log.{LogRecord, LogRecordExtendSS}
import com.ald.stat.module.uv.{UVDepthStat, UVDurationStat}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink}
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.ald.stat.job.task.UserVisitDurationTask


import scala.collection.mutable.ArrayBuffer

/**
  * Created by spark01 on 6/11/18.
  */
object gcsTest extends App{

  val prefix = "jobVS"

  var dateStr = ComputeTimeUtils.getDateStr(new Date())
  var taskId = "task-1"
  if (args.length == 2) {
    dateStr = args(0)
    taskId = args(1)
  }

  val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    .set("spark.executor.cores", "10")
    .setMaster(ConfigUtils.getProperty("spark.master.host"))
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))


  val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
    val kafkaProducerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
      p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      p
    }
    ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
  }

  val baseRedisKey = "rt:userVisit"

  //广播
  val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
  val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
  val topic= ConfigUtils.getProperty("kafka." + prefix + ".topic") //gcs:kafka.jobLink.

  val stream = SQLKafkaConsume.getStream(ssc, group,topic)


  stream.foreachRDD(rdd => {

    val logRecordRdd = rdd.mapPartitions(
      par => {

        val recordsRdd = ArrayBuffer[LogRecordExtendSS]()
        par.foreach(line => {


          var logRecord = LogRecordExtendSS.line2Bean(line.value())
          if(logRecord != null && logRecord.st !=null && StringUtils.isNotBlank(logRecord.dr)
            && !"NaN".equals(logRecord.dr)){
            recordsRdd += logRecord
          }
        })
        recordsRdd.iterator
      }

    ).cache()

    //==================================读取kafka数据=====================


    //    TerminalTask.dailyStat(baseRedisKey,taskId,dateStr,logRecordRdd,kafkaProducer,redisPrefix)
//    val uuCountRDD_eventAnalyse = UUStat.statIncreaseCache(baseRedisKey, dateStr,DailyUuSubDimensionKey, logRecordRdd, redisPrefix)


//    println(logRecordRdd.sample(true,0.1,1).count())
//    var uv_userVisitDepth = UVDepthStat.statIncreaseCacheExtendUU(baseRedisKey,dateStr,DailyUVDepthUUSubDimensionKeyExtend,logRecordRdd,redisPrefix)

//    var at_userVisetDepth = UVDepthStat.statIncreaseCacheExtendAT(baseRedisKey,dateStr,DailyUVDepthATSubDimensionKeyExtend,logRecordRdd,redisPrefix)

//    val uv_userVisitDuration = UVDurationStat.statIncreaseCacheExtendUU(baseRedisKey,dateStr,DailyUVDurationUUSubSimensionKeyExtend,logRecordRdd,redisPrefix)

//    val at_userVisitDuration = UVDurationStat.statIncreaseCacheExtendAT(baseRedisKey,dateStr,DailyUVDurationATSubDimensionKeyExtend,logRecordRdd,redisPrefix)

     UserVisitDurationTask.dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecordExtendSS], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String])
    /** ------------------------------lang----------------------------- */
    //    val tempSessMergeResult_lang = SessionStatImpl.stat(logRecordRdd, DailyLangSessionSubDimensionKey, SessionBaseImpl)
    //    //session处理
    //    val sessionRDD_lang = SessionStatImpl.doCache(baseRedisKey,taskId,dateStr, tempSessMergeResult_lang, DailyLangSessionSubDimensionKey, redisPrefix)
    //    //uv test=ok
    //    val uvRDD_lang = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyLangUidSubDimensionKey, logRecordRdd, redisPrefix)
    //    //open count  test=ok
    //    val opRDD_lang = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyLangSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //    //pv  test=ok
    //    val pvRDD_lang = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyLangDimensionKey, logRecordRdd, redisPrefix)
    //    //最终结果集
    //    val finalRDD_lang = pvRDD_lang.join(uvRDD_lang).join(opRDD_lang).join(sessionRDD_lang)



//    println(at_userVisitDuration.sample(true,0.1,1).collect().toList)
  })


  ssc.start()
  ssc.awaitTermination()

}
