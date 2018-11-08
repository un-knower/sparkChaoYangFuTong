package com.ald.stat.kafka.hbase

import com.ald.stat.utils.ConfigUtils
import com.ald.stat.utils.DBUtils.{getConnection, use}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object Write2Mysql {

  val logger = LoggerFactory.getLogger("write2mysql wait 120 ms")

  def main(args: Array[String]): Unit = {
    //  创建SparkConf

    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
//          .setMaster(ConfigUtils.getProperty("spark.master.host"))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))

    val group = "KAFKA2HBASE_SQL"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = ConfigUtils.getProperty("kafka.mysql.sql.topic")
    logger.info("topic:" + topic.toString)
    val topics = Array(topic)
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // 创建流
    dStream.foreachRDD { fr =>
      fr.foreachPartition(xx => {
        //        val batchSize = 1000
        var count: Int = 0
        use(getConnection) {
          connection => {
            //            connection.setAutoCommit(false)
            use(connection.createStatement()) {
              stmt => {
                xx.foreach { line =>
                  println(line.value())
                  try {
                    count += stmt.executeUpdate(line.value)
                    //                    stmt.addBatch(line.value())
                  } catch {
                    case t: Throwable => {
                      logger.error("save to db error!!!!!! stmt sql.............." + line, t)
                    }
                  }
                  //                  if (count % batchSize == 0) {
                  //                  try {
                  //                    //                      stmt.executeBatch
                  //                    stmt.execute(line.value)
                  //                    connection.commit()
                  //                    Thread.sleep(200)
                  //                  } catch {
                  //                    case t: Throwable => {
                  //                      logger.error("save to db error!!!!!! stmt sql.............." + line, t)
                  //                    }
                  //                  }
                  //                  count += 1
                }

                //                try {
                //                 //                  stmt.executeBatch
                //                  stmt.execute(line.value)
                //                  //                  Thread.sleep(10)
                //                } catch {
                //                  case e: Throwable => {
                //                    logger.error("save to db error!!!!!! executeBatch commit ..............", e)
                //                    connection.rollback()
                //                  }
                //                }
              }
            }
            //                        connection.setAutoCommit(true)
          }
            count
        }
        logger.info("affected count:" + count)
      })
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
