package com.ald.stat.job.task

import java.security.MessageDigest
import java.sql.Timestamp
import java.time.LocalDateTime

import com.ald.stat.component.dimension.{DimensionKey, DimensionKeyExtend}
import com.ald.stat.log.LogRecord
import com.ald.stat.module.at.{ATStat, UUStat}
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.ald.stat.component.dimension.event.eventAnalyse._
import org.apache.spark.sql.SparkSession
import com.ald.stat.module.ct.CtStat
import com.ald.stat.component.dimension.event.eventParam.DailyCtDimensionKeyExtend
import com.ald.stat.log.EmojiFilter

/**
  * Created by spark01 on 6/4/18.
  */
object EventTask  extends TaskTrait{

  def dailyStat(sparkSession: SparkSession, baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecord], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit = {

    /* ------------------------------eventAnalyse----------------------------- */
    //gcs:uu的数目的统计
    val uuCountRDD_eventAnalyse = UUStat.statIncreaseCache(baseRedisKey, dateStr,DailyUuSubDimensionKey, logRecordRdd, redisPrefix)
    val atCountRdd_eventAnalyse =ATStat.statIncreaseCache(baseRedisKey, dateStr,DailyAtSubDimensionKey, logRecordRdd, redisPrefix)

    //gcs:最后的结果的类型：RDD[(DimensionKey, ((Long, String), (Long, String)))]
    val finalRDD_eventAnalyse = uuCountRDD_eventAnalyse.join(atCountRdd_eventAnalyse) //gcs:uuCount在前，atCount在后
    writeKafkaEvent(kafkaProducer,finalRDD_eventAnalyse)

    /* ------------------------------eventParams----------------------------- */

    val ctCountRDD =CtStat.logReRdd2LogReExtentRdd(logRecordRdd)
    val ctCountRDD_eventParams = CtStat.statIncreaseCache(baseRedisKey,dateStr,DailyCtDimensionKeyExtend,ctCountRDD,redisPrefix)
    writeKafkaParam(kafkaProducer,ctCountRDD_eventParams)
  }


  /**
    * 入kafka
    *
    * @param kafkaProducer
    * @param rdd
    */
  def writeKafkaEvent(kafkaProducer: Broadcast[KafkaSink[String, String]], rdd: RDD[(DimensionKey, ((Long, String), (Long, String)))]) {
    rdd.foreachPartition(par => {

      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => { //gcs:(DimensionKey, ((Long, String), (Long, String)))
        //从DimensionKey中获取

        val splits = row._1.toString.split(":") //gcs:ak + day + tp

        val app_key = splits(0)
        val day = splits(1)
        var tp_value = splits(2)

        val trigger_user_count =row._2._1._1 //gcs:trigger_user_count =uu
        val trigger_count =row._2._2._1  //gcs:trigger_count =at
        val event_key = toHex(app_key+tp_value)
        var avg_trigger_count =0f    //gcs:at/uu
        if (0 != trigger_user_count){
          avg_trigger_count = trigger_count.toFloat / trigger_user_count.toFloat
        }


        val updateAt = Timestamp.valueOf(LocalDateTime.now())
        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_daily_event
             |(
             |    app_key,
             |    day,
             |    ev_id,
             |    event_key,
             |    trigger_user_count,
             |    trigger_count,
             |    avg_trigger_count,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$tp_value",
             |    "$event_key",
             |    "$trigger_user_count",
             |    "$trigger_count",
             |    "$avg_trigger_count",
             |    "$updateAt"
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |ev_id="$tp_value",
             |event_key="$event_key",
             |trigger_user_count="$trigger_user_count",
             |trigger_count="$trigger_count",
             |avg_trigger_count="$avg_trigger_count",
             |update_at="$updateAt"
            """.stripMargin
//        kafka.send("mysql_test", sqlInsertOrUpdate) //gcs:注释
        println(sqlInsertOrUpdate)
      })
    })
  }


  def  writeKafkaParam(kafkaProducer: Broadcast[KafkaSink[String, String]], rdd:  RDD[(DimensionKeyExtend, Long)]): Unit ={

    rdd.foreachPartition(par => {

      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => { //gcs:(DimensionKey, ((Long, String), (Long, String)))
        //从DimensionKey中获取
        val splits = row._1.toString.split(":") //gcs:ak + day + tp +ss
        val app_key = splits(0)
        val day = splits(1)
        val ev_id = splits(2) //gcs:tp
        val event_key = toHex(app_key + ev_id)   //gcs:将app_key+ev_id 进行MD5的加密
        val eventKeySplit = splits(3).split("-ald-")


          val ev_paras_name = EmojiFilter.emojiJudgeAndFilter(str255(eventKeySplit(0))) //gcs:ss.key
          val ev_paras_value = EmojiFilter.emojiJudgeAndFilter(str255(eventKeySplit(1))) //gcs:ss.value
          val ev_paras_count =row._2 //gcs:ev_paras_count 事件参数的个数

          val updateAt = Timestamp.valueOf(LocalDateTime.now())
          val sqlInsertOrUpdate =
            s"""
               |insert into aldstat_event_paras
               |(
               |    app_key,
               |    day,
               |    ev_id,
               |    event_key,
               |    ev_paras_name,
               |    ev_paras_value,
               |    ev_paras_count,
               |    update_at
               |)
               |values(
               |    "$app_key",
               |    "$day",
               |    "$ev_id",
               |    "$event_key",
               |    "$ev_paras_name",
               |    "$ev_paras_value",
               |    "$ev_paras_count",
               |    "$updateAt"
               |)
               |on DUPLICATE KEY UPDATE
               |app_key="$app_key",
               |day="$day",
               |ev_id="$ev_id",
               |event_key="$event_key",
               |ev_paras_name="$ev_paras_name",
               |ev_paras_value="$ev_paras_value",
               |ev_paras_count=ev_paras_count+"$ev_paras_count",
               |update_at="$updateAt"
            """.stripMargin
                  kafka.send("mysql_test", sqlInsertOrUpdate)


      })
    })
  }

  /**<br>gcs:<br>
    * 将一个字节流当中的每一个字节都变成 "%02x"的格式
    * */
  def toHex(bytes: String): String = {


    var secure = MessageDigest.getInstance("MD5").digest(bytes.getBytes("UTF-8"))
    secure.map("%02x".format(_)).mkString("")
  }


  /**<br>gcs:<br>
    * 限制存储在数据库当中的b0的长度最多为255个字符
    * @param str 用于提取前255个字符的数据
    * @return 将str的前255个字符提取出来
    * */
  def  str255(str:String):String={
    var str0=str
    if(str !=null && str.length>255)
      str0=str.substring(0,254)
    //      if(str!=null && str.equals("凉感网眼V领T恤-男"))
    //        str0=null
    str0
  }

}
