package com.ald.stat.kafka.hbase

import java.util.Properties

import com.ald.stat.utils.ConfigUtils
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object KafkaProducer {

  val topic = "mysql_test"
  val brokers = ConfigUtils.getProperty("kafka.host")
  val zkHost = ConfigUtils.getProperty("zookeeper.host")
  val props = new Properties()

  props.put("bootstrap.servers", brokers)
  props.put("acks", "all")
  props.put("retries", 0.toString)
  props.put("batch.size", 1000.toString)
  props.put("linger.ms", 1.toString)
  props.put("buffer.memory", 512000000.toString)

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val zkClient = new ZkClient(s"$zkHost", 6000, 60000)
  val zkConnection = new ZkConnection(zkHost);

  val zkUtils = new ZkUtils(zkClient, zkConnection = zkConnection, false)
  val producer = new KafkaProducer[String, String](props)

  if (!AdminUtils.topicExists(zkUtils, topic)) {
    createTopic(topic, 1)
  }

  def createTopic(topic: String, partitions: Int): Unit = {
    AdminUtils.createTopic(zkUtils, topic, partitions, 1)
  }

  def write(text: String): Unit = {
    println(text)
    val sqlData = new ProducerRecord[String, String](topic, text)
    producer.send(sqlData)
  }
}