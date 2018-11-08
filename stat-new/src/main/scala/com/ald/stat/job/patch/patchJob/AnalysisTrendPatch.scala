package com.ald.stat.job.patch.patchJob

import java.util.{Date, Properties}

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AbstractBaseJob
import com.ald.stat.job.patch.patchTask.TrendPatchAllTask
import com.ald.stat.kafka.hbase.KafkaConsume
import com.ald.stat.log.LogRecord
import com.ald.stat.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable.ArrayBuffer

/*
*gcs:
*这个是趋势分析实时的计算的模块用来补数据的。
* 具体这段代码中会涉及到kafka的offset的存储和提取。
* 我们的kafka的offset是存储在redis当中的，这一点一定要切记啊
*/
object AnalysisTrendPatch extends AbstractBaseJob {

  val prefix = "default"  //gcs:定义prefix
  val offsetPrefix = "trend_offset"
  val baseRedisKey = "rt";

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //==========================================================1
    /*
    *gcs:
    *定义task的ID
    *&
    *分区的数目
    */
    val taskId = "task-1"
    var partitionNumber = 20
    var dateStr = ComputeTimeUtils.getDateStr(new Date()) //gcs:获得当前的时间
    var patchType = GlobalConstants.PATCH_TYPE_CACHE_LATEST  //gcs:补偿到上次实时失败时缓存中最大offset。这是什么意思啊？？？

    if (args.length >= 1) {
      if (args(0).toInt > 0) {
        try {
          partitionNumber = args(0).toInt  //gcs:默认的分区的数目是20.当然我们也是支持接受自定义分区
        } catch {
          case e: NumberFormatException => {
            println(s"$args(0)")
            e.printStackTrace()
          }
        }
      }
      if (args.length >= 2) {
        dateStr = args(1)
      }
      //判断补偿逻辑
      if (args.length >= 3) {
        try {
          patchType = args(2)
        } catch {
          case e: NumberFormatException => {
            println(s"$args(2)")
            e.printStackTrace()
          }
        }
      }
    }

    println(s"$partitionNumber,$dateStr,$patchType")

    //gcs:首先获得类的名字，然后用这个类的名字创建一个sparkConf对象。
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .setMaster(ConfigUtils.getProperty("spark.master.host")) //gcs:指定master的地址。也就是连接master的地址
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //gcs:指定使用Kyro的序列化方式
      .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    //==========================================================2
    /*
    *gcs:
    *当sparkConf对象创建完成之后，此时就可以来
    */
    rddPatch(sparkConf, taskId, dateStr, partitionNumber, patchType)
  }

  /**
    *
    * @param sparkConf
    * @param partitionNumber
    */
  def rddPatch(sparkConf: SparkConf, taskId: String, dateStr: String, partitionNumber: Int, patchType: String): Unit = {

    //gcs:用sparkConf对象创建出一个sparkContext对象
    val sparkContext = new SparkContext(sparkConf)

    //gcs:创建一个kafka的Producer对象
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //gcs:这里为什么还要创建一个grey的kafka的producer对象呢？？？
    val grey_kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ConfigUtils.getProperty("grey_kafka.host"))
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //gcs:从广播变量中获得redis的前缀
    val redisPrefix = sparkContext.broadcast[String](prefix)
    val group = ConfigUtils.getProperty("kafka." + prefix + ".group.id") //gcs:获得redis的group的ID
    val topic = ConfigUtils.getProperty("kafka." + prefix + ".topic") //gcs:获得topic的名字

    //gcs:将baseRedisKey，还有这个offsetPrefix传进去的目的获得在minOffset和lastOffset范围之间的RDD对象吗？ 然后再对这个范围的RDD进行操作
    val stream_rdd = KafkaConsume.rddFromOffsetRangeToPatch(sparkContext, topic, group, baseRedisKey, offsetPrefix)
    handleNewUserForOldSDK(stream_rdd, baseRedisKey, prefix)//标记新用户

    //gcs:对我们的原始的kafka日志中的数据进行筛选和处理。因为流过kafka的原始日志是有脏数据的，比方说没有ak,at,ev,uu,et,dr等字段
    val logRecordRdd: RDD[LogRecord] = stream_rdd.repartition(partitionNumber).mapPartitions(par => {
      val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
      val resource = redisCache.getResource
      val dateLong = ComputeTimeUtils.getTodayDayTimeStamp(new Date())
      val recordsRdd = ArrayBuffer[LogRecord]()
      try{
        par.foreach(line => {
          val logRecord = LogRecord.line2Bean(line.value())
          if (logRecord != null &&
            StringUtils.isNotBlank(logRecord.ak) &&
            StringUtils.isNotBlank(logRecord.at) &&
            StringUtils.isNotBlank(logRecord.ev) &&
            StringUtils.isNotBlank(logRecord.uu) &&
            StringUtils.isNotBlank(logRecord.et) &&
            StringUtils.isNotBlank(logRecord.dr) &&
            StrUtils.isInt(logRecord.dr)&&
            StringUtils.isNotBlank(logRecord.pp)) {
            logRecord.et = ComputeTimeUtils.checkTime(logRecord.et.toLong,dateLong) //时间校正

            //标记新用户  && logRecord.v < "7.0.0"
            if (logRecord.v != null) {
              markNewUser(logRecord, resource, baseRedisKey)
            }
            //如果是7.0.0以上的版本，则去除掉ev=app的数据，避免多算新用户数
            if(logRecord.v >= "7.0.0"){
              if(logRecord.ev != "app"){
                recordsRdd += logRecord
              }
            }else{
              recordsRdd += logRecord
            }
          }
        })
      }catch {
        case jce: JedisConnectionException => jce.printStackTrace()
      } finally {
        if (resource != null) resource.close()
      }
      recordsRdd.iterator
    }).cache()

    val dateStr = ComputeTimeUtils.getDateStr(new Date())
    //趋势分析
    val grap_map = cacheGrapUser("online_status") //缓存灰度用户上线状态
    val isOnLine = isOnline("online_status")

    //gcs:之后将处理完成后的数据之后，将清洗完成的数据供Trend的指标分析函数取调用
    //趋势分析
    TrendPatchAllTask.hourlyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
    TrendPatchAllTask.dailyStat(baseRedisKey, taskId, dateStr, logRecordRdd, kafkaProducer, grey_kafkaProducer, grap_map, isOnLine, redisPrefix)
  }

}
