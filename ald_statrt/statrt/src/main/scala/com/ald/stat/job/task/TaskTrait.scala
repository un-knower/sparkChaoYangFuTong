package com.ald.stat.job.task

import com.ald.stat.utils.{ConfigUtils, KafkaSink}

/**
  * 基础task
  */
trait TaskTrait {

  val kafka_topic = ConfigUtils.getProperty("kafka.mysql.sql.topic")


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

}
