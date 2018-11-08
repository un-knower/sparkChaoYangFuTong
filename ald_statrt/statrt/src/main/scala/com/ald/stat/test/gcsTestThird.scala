package com.ald.stat.test

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.time.LocalDateTime
import java.util.{Date, Properties}

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.session.SessionSum
import com.ald.stat.job.AnalysisLink.prefix
import com.ald.stat.job.task.TerminalTask
import com.ald.stat.kafka.hbase.SQLKafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.test.gcsTestSecond.{args, sparkConf}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by spark01 on 6/4/18.
  */
class gcsTestThird extends App{



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

  JDBCRDD
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

  val baseRedisKey = "rt:terminal"

  //广播
  val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
  val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
  val topic= ConfigUtils.getProperty("kafka." + prefix + ".topic")
  val stream = SQLKafkaConsume.getStream(ssc, group,topic)


  stream.foreachRDD(rdd => {

    val logRecordRdd = rdd.mapPartitions(
      par => {
        val recordsRdd = ArrayBuffer[LogRecord]()
        val str2 = ArrayBuffer[LogRecord]()
        //        recordsRdd ++= str2
        par.foreach(line => {

          var logRecord = LogRecord.line2Bean(line.value())
          if(logRecord != null && logRecord.st !=null){
            recordsRdd += logRecord
          }
        })
        recordsRdd.iterator
      }
    ).cache()

    TerminalTask.dailyStat(baseRedisKey,taskId,dateStr,logRecordRdd,kafkaProducer,redisPrefix)

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



    //    val tempSessMergeResult_nt = SessionStatImpl.stat(logRecordRdd, DailyNtSessionSubDimensionKey, SessionBaseImpl)
    //    //session处理
    //    val sessionRDD_nt = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_nt, DailyNtSessionSubDimensionKey, redisPrefix)
    //
    //    //uv
    //    val uvRDD_nt = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyNtUidSubDimensionKey, logRecordRdd, redisPrefix)
    //
    //    //open count
    //    val opRDD_nt = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyNtSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //
    //    //pv
    //    val pvRDD_nt = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyNtDimensionKey, logRecordRdd, redisPrefix)
    //    //最终结果集
    //    val finalRDD_nt = pvRDD_nt.join(uvRDD_nt).join(opRDD_nt).join(sessionRDD_nt)



    //    val tempSessMergeResult_sv = SessionStatImpl.stat(logRecordRdd, DailySvSessionSubDimensionKey, SessionBaseImpl)
    //    //session处理
    //    val sessionRDD_sv = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, tempSessMergeResult_sv, DailySvSessionSubDimensionKey, redisPrefix)
    //
    //    //uv
    //    val uvRDD_sv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailySvUidSubDimensionKey, logRecordRdd, redisPrefix)
    //
    //    //open count
    //    val opRDD_sv = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailySvSessionSubDimensionKey, logRecordRdd, redisPrefix)
    //    //pv
    //    val pvRDD_sv = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailySvDimensionKey, logRecordRdd, redisPrefix)
    //    //最终结果集
    //    val finalRDD_sv = pvRDD_sv.join(uvRDD_sv).join(opRDD_sv).join(sessionRDD_sv)
    //
    //    writeKafka(null,null,finalRDD_sv)






    //gcs:主要测试完这两个函数就可以了
    //    val resultDaily = OpenCountStat.opencountOfDailyIncreaseCache(baseRedisKey, "1526897455000", "taskId", OpenCountOfDailyDimensionKey, logRecordRdd)



    //    val resultHour =  OpenCountStat.opencountOfDailyIncreaseCache(baseRedisKey, "1526897455000", "taskId", OpenCountOfHourSubDimensionKey, logRecordRdd)

  })


  ssc.start()
  ssc.awaitTermination()


  def writeKafka(dimension_name: String, kafkaProducer: Broadcast[KafkaSink[String, String]], rdd: RDD[(DimensionKey, (((Long, Long), Long), SessionSum))]): Unit ={


    rdd.foreachPartition(par => {

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
        if (visitor_count != 0) {
          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
        }
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
             |avg_stay_time=round(total_stay_time/visitor_count,2),
             |one_page_count=one_page_count+"$one_page_count",
             |bounce_rate=round(one_page_count/open_count,2),
             |update_at="$updateAt"
            """.stripMargin //gcs:出去"|"之前的所有的 |号



      })
    })
  }

}
