package com.ald.stat.test

import java.util.Properties

import com.ald.stat.utils.ConfigUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}


object TestKafka extends App {

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
  //topics
  println(zkUtils.getAllPartitions().filter(r => r.topic.equals(topic)))


}
