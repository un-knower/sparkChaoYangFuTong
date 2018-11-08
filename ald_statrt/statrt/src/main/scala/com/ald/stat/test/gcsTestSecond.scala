package com.ald.stat.test


import java.util.{Date, Properties}

import com.ald.stat.component.dimension.event.eventAnalyse.{DailyAtSubDimensionKey, DailyUuSubDimensionKey}
import com.ald.stat.component.dimension.event.eventParam.DailyCtDimensionKeyExtend
import com.ald.stat.job.task.EventTask.{writeKafkaEvent, writeKafkaParam}
import com.ald.stat.kafka.hbase.SQLKafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.module.at.{ATStat, UUStat}
import com.ald.stat.module.ct.CtStat
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by spark01 on 6/1/18.
  */
object gcsTestSecond  extends App{


  val prefix = "rt_test"

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

  val baseRedisKey = "rt:event"

  //广播
  val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
//  val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
  val group ="tr_test_UserVisiter"
//  val topic= ConfigUtils.getProperty("kafka." + prefix + ".topic")
  val topic = "rt_test"
  val stream = SQLKafkaConsume.getStream(ssc, group,topic)


  stream.foreachRDD(rdd => {

    val logRecordRdd = rdd.mapPartitions(
      par => {
        val recordsRdd = ArrayBuffer[LogRecord]()

        par.foreach(line => {

          var logRecord = LogRecord.line2Bean(line.value())
          if(logRecord != null && logRecord.st !=null){
            recordsRdd += logRecord
          }
        })
        recordsRdd.iterator
      }
    ).cache()


//    TerminalTask.dailyStat(baseRedisKey,taskId,dateStr,logRecordRdd,kafkaProducer,redisPrefix)
//    val uuCountRDD_eventAnalyse = UUStat.statIncreaseCache(baseRedisKey, dateStr,DailyUuSubDimensionKey, logRecordRdd, redisPrefix)

//    val atCountRdd_eventAnalyse =ATStat.statIncreaseCache(baseRedisKey, dateStr,DailyAtSubDimensionKey, logRecordRdd, redisPrefix)


    val ctCountRDD =CtStat.logReRdd2LogReExtentRdd(logRecordRdd)
//    val ctCountRDD_eventParams = CtStat.statIncreaseCache(baseRedisKey,dateStr,DailyCtDimensionKeyExtend,ctCountRDD,redisPrefix)
//
//    writeKafkaParam(kafkaProducer,ctCountRDD_eventParams)

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





    println(s"------------------------0-------------------------")
    println(ctCountRDD.sample(true,0.1,1).count())
  })


  ssc.start()
  ssc.awaitTermination()


}
