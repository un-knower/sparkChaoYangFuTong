package com.ald.stat.job

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.task.TerminalTask
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, KafkaSink}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 终端分析
  */
object AnalysisTerminal {

  val prefix = "jobTerminal"
  lazy val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
  val baseRedisKey = "rt";

  def main(args: Array[String]): Unit = {

    val taskId = "task-1"
    var partitionNumber = 20
    if (args.length >= 1) {
      if (args(0).toInt > 0) {
        partitionNumber = args(0).toInt
      }
    }

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      //      .set("spark.executor.cores", "10")
      //      .setMaster(ConfigUtils.getProperty("spark.master.host"))
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
    val redisPrefix = ssc.sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id")
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic")
    val stream = KafkaConsume.getStream(ssc, group, topic)

    // TODO:   把新用户的会话存入Redis，然后对比，将所有新用户的回话记录的ifo字段改成true

    stream.repartition(partitionNumber)
      .foreachRDD(rdd => {
        val logRecordRdd = rdd.mapPartitions(
          par => {
            val recordsRdd = ArrayBuffer[LogRecord]()
            par.foreach(line => {
              //添加场景group id
              val logRecord = LogRecord.line2Bean(line.value())
              if (logRecord != null &&
                StringUtils.isNotBlank(logRecord.ak) &&
                StringUtils.isNotBlank(logRecord.at) &&
                StringUtils.isNotBlank(logRecord.ev) &&
                StringUtils.isNotBlank(logRecord.uu) &&
                StringUtils.isNotBlank(logRecord.et) &&
                StringUtils.isNotBlank(logRecord.dr) &&
                StringUtils.isNotBlank(logRecord.pp) &&
                StringUtils.isNotBlank(logRecord.lang) &&
                StringUtils.isNotBlank(logRecord.nt) &&
                StringUtils.isNotBlank(logRecord.sv) &&
                StringUtils.isNotBlank(logRecord.wsdk) &&
                StringUtils.isNotBlank(logRecord.wv) &&
                StringUtils.isNotBlank(logRecord.wvv) &&
                StringUtils.isNotBlank(logRecord.ww) &&
                StringUtils.isNotBlank(logRecord.wh)
              ) {
                recordsRdd += logRecord
              }
            })
            recordsRdd.iterator
          }).cache()
        val dateStr = ComputeTimeUtils.getDateStr(new Date())
        TerminalTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, redisPrefix)
      })
    ssc.start()
    ssc.awaitTermination()
  }

}

