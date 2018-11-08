package com.ald.stat.job.task

import java.util

import com.ald.stat.utils.{ConfigUtils, KafkaSink}

/**
  * 基础task
  */
trait TaskTrait {

  val kafka_topic = ConfigUtils.getProperty("kafka.mysql.sql.topic")
  val grey_kafka_topic = ConfigUtils.getProperty("grey_kafka.mysql.sql.topic")


  /**
    *
    * @param kafka
    * @param sql
    */
  def sendToKafka(kafka: KafkaSink[String, String], sql: String): Unit = {
    if ("true" == ConfigUtils.getProperty("write.to.kafka")) {
      kafka.send(kafka_topic, sql)
    } else {
      println(sql)
    }
  }

  /**
    * 写入灰度kafka
    * @param kafka
    * @param sql
    */
  def sendToGreyKafka(kafka: KafkaSink[String, String], sql: String): Unit = {
    if ("true" == ConfigUtils.getProperty("write.to.kafka")) {
      kafka.send(grey_kafka_topic, sql)
    } else {
      println(sql)
    }
  }


  /**
    * 判断是否是上线用户
    * true：入生产kafka
    * false:入灰度kafka
    * @param grey_map   用户上线状态信息
    * @param app_key    用户ak
    * @return
    */
  def isGrey(grey_map:util.HashMap[String, String],app_key:String):Boolean={
    val online_status = grey_map.get(app_key)
    if( online_status != null){
      if (online_status == "1"){
        true
      }else{
        false
      }
    }else{
      false
    }
  }



}
