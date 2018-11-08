package com.ald.stat.test

import java.util.Properties

import com.ald.stat.utils._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.State
import org.apache.spark.{SparkConf, SparkContext}

import scala.beans.BeanProperty

object TestApp extends App {

  val sc = sparkContext("TestApp", true)
  val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
    val kafkaProducerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", ConfigUtils.getProperty("kafka.host"))
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }
    sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
  }

  val stateUpdateFunction = (key: ClientKeys, value: Option[Long], stateData: State[Long]) => {

    val n = value.get match {
      case v => v
      case _ => 0l
    }
    val r = stateData.get() match {
      case k => stateData.update(n + k); (key, n + k)
      case _ => (key, n)
    }

    //    return userModel/
    // / Send model downstream
    Some(r)
    //    Some(r)
  }

  var count = 0


  private def sparkContext(appName: String, isLocal: Boolean): SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")

    if (isLocal) {
      sparkConf.setMaster("local[*]")
    }
    new SparkContext(sparkConf)
  }
}


class Egg() extends Copyable with Serializable {
  @BeanProperty
  var p1: String = "dd"
  var p2: Int = 0

  override def toString = s"Egg($p1,$p2)"
}

class SubEgg extends Egg {
  var p3: String = "d2"

  override def toString = s"SubEgg($p1,$p2,$p3)"
}


