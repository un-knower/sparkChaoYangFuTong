package com.ald.stat.test

import java.util.Properties

import com.ald.stat.utils.ConfigUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source


//生产测试数据
object KafkaTestDataProducer extends App {

  val topic = "online_new"
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
  var count = 0
  Source.fromFile("/Users/mike/Desktop/test-et.json").getLines().foreach(line => {
    try {
      producer.send(new ProducerRecord[String, String](topic, line))
    } catch {
      case e: Throwable => {
        e.printStackTrace()
      }
    }
    count += 1
    println("count:" + count)
  })
}
