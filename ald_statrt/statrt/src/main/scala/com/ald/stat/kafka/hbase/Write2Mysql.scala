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

/**
* <b>author:</b> gcs <br>
* <b>data:</b> 18-7-10 <br>
* <b>description:</b><br>
  *   这个是将Kafka当中的数据使用MySql进行存储
* <b>param:</b><br>
* <b>return:</b><br>
*/
object Write2Mysql {

  val logger = LoggerFactory.getLogger("write2mysql wait 120 ms")

  def main(args: Array[String]): Unit = {
    //  创建SparkConf

    //==========================================================1
    /*
    *gcs:
    *创建一个sparjConf对象
    */
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .setMaster(ConfigUtils.getProperty("spark.master.host"))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))

    //==========================================================2
    /*
    *gcs:
    *这个group是kafka的consumer的分组
    */
    val group = "KAFKA2HBASE_SQL"

    /*
    *gcs:
    *创建一个Kafka的配置项
    */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //==========================================================3
    /*
    *gcs:
    *获得Kafka存储的topic
    */
    val topic = ConfigUtils.getProperty("kafka.mysql.sql.topic")
    logger.info("topic:" + topic.toString)
    val topics = Array(topic)

    //==========================================================4
    /*
    *gcs:
    *创建一个Kafka的流。这样就可以批量地从Kafka当中读取数据了
    */
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    //==========================================================5
    /*
    *gcs:
    *开始批量地处理每一个batch RDD当中的Kafka当中的数据
    */
    // 创建流
    dStream.foreachRDD { fr =>
      fr.foreachPartition(xx => {
        //        val batchSize = 1000
        var count: Int = 0
        //==========================================================6
        /*
        *gcs:
        *将batchRDD当中获得的每一条数据进行插入到MySql数据库的操作
        * 这样利用Kafka的batch RDD的产生的间隔可以调整的特点，达到负载均衡的目的
        * 我可以将我的Put操作，进行序列化之后，统一地写入到Kafka当中。之后再写一个程序，不断地从Kafka中把存储进去的Put对象读出来，
        * 再进行反序列化之后，批量地写入到HBase当中。在写入的过程中，我还可以对RDD进行重新的分区，来缩短在存入Hbase数据库时的压力
        */
        use(getConnection) {
          connection => {
            //            connection.setAutoCommit(false)
            //==========================================================7
            /*
            *gcs:
            *获得MySql的连接。之后将每一个batch RDD当中的数据都按照count值，批量地存储进MySql当中
            */
            use(connection.createStatement()) {
              stmt => {
                xx.foreach { line =>
                  println(line.value())
                  try {
                    //==========================================================8
                    /*
                    *gcs:
                    *
                    */
                    count += stmt.executeUpdate(line.value)  //gcs:这个executeUpdate的返回值是此时的stmt.executeUpdate(line.value)影响的MySql数据库中的行数
                    //                    stmt.addBatch(line.value())
                  } catch {
                    case t: Throwable => {
                      logger.error("save to db error!!!!!! stmt sql.............." + line, t)
                    }
                  }

                  //==========================================================9
                  /*
                  *gcs:
                  *这里的代码已经被注释掉了。但是大致的逻辑是将所有的需要执行的SQL语句先收集起来，不执行
                  * 一直等count的值达到了执行batchSize，再将这些SQL语句执行批量的插入操作。等批量的MySql的插入操作执行完成之后，让这个线程沉睡200ms。
                  * 以使得MySql的批量插入的执行完成。
                   * 我在HBase的批量的Put插入的操作时，也可以采用这种方法。设定一个阈值，等Put的个数达到这个阈值之后，就执行Put的批量的插入操作。
                   * 之后,再重新计数，重新连接HBase和执行下一个周期的批量的插入的操作。
                   * 在这个过程中一旦发现了在一个批次的RDD中，所有的数据计算完不成的话，就可以动态地调整batch RDD的产生一个批次的时间
                   *
                  //==========================================================10
                  /*
                  *gcs:
                  *那如果要写我们这个存储到HBase的程序，我们就需要将这个任务分为3个第一个依赖的任务。
                  * 第1个任务，将我们第一次etl处理完的数据读取出来，将有用的数据提取出来，去重，创建Put对象。将这些Put对象进行序列化，存储到Kafka当中
                  * 第2个任务，是将我们的存储到Kafka当中的数据读取出来，进行反序列化的操作。就像这个从Kafka将数据读取出来，之后存储到MySql当中的过程一样，
                  * 将Kafka当中的数据读取出来了，之后将Put对象进行反序列化，存储到HBase当中
                  * 第3个任务，就是将原始的数据再重新计算一遍。将我们的数据从HBase当中提取出来，将url使用openId或者Url进行替换
                  */
                  */
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
