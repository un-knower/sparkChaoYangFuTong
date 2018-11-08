package com.ald.stat.kafka.hbase

import com.ald.stat.utils.ConfigUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.slf4j.LoggerFactory

/**
  * Created by root on 2018/5/18.
  */
object SQLKafkaConsume {
  val logger = LoggerFactory.getLogger("testStreaming")

  /**<br>gcs:
    * @param ssc 创建的StreamingContext对象 <br>
    * @param group 创建的groupId <br>
    * @param topic 用于读取数据的topic对象 <br>
    * <br>*/
  def getStream(ssc: StreamingContext, group: String, topic: String): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigUtils.getProperty("kafka.host"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Set(topic)
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    dStream
  }

  //  def main(args: Array[String]) {
  //    var test="{\"ev\":\"page\",\"client_ip\":\"112.31.140.14\",\"ak\":\"1548284aae6f48b854b0a96a730d4bed\",\"wh\":\"570\",\"scene\":1089,\"issticky\":false,\"pp\":\"pages\\/game\\/game\",\"pr\":\"3\",\"pm\":\"MX5\",\"ec\":\"0\",\"lang\":\"zh_CN\",\"wsr\":\"{\\\"scene\\\":1089,\\\"path\\\":\\\"pages\\/index\\/index\\\",\\\"query\\\":{},\\\"topBarStatus\\\":false,\\\"isSticky\\\":false}\",\"st\":\"1526313601890\",\"province\":\"安徽\",\"life\":\"show\",\"v\":\"6.1.2\",\"sc\":\"undefined\",\"lp\":\"pages\\/component1\\/pages\\/pass31\\/pass31\",\"dr\":\"5\",\"ww\":\"360\",\"lua_wechat_version\":\"6.6.6.1300(0x26060637)\",\"at\":\"15263135855302498635\",\"country\":\"中国\",\"rq_c\":\"4\",\"city\":\"\",\"server_time\":1526313601.987,\"spd\":\"undefined\",\"topbarstatus\":false,\"wvv\":\"android\",\"query\":{},\"nt\":\"wifi\",\"lat\":\"undefined\",\"sv\":\"Android 5.1\",\"path\":\"pages\\/index\\/index\",\"wv\":\"6.6.6\",\"wsdk\":\"2.0.6\",\"uu\":\"15262235390126980831\",\"lng\":\"undefined\"}/detail\\/detail\",\"relaunch\":true,\"ag_cid\":\"5abc554dd459353b1eec0e96\",\"pr\":\"3\",\"pm\":\"NEM-AL10\",\"ec\":\"0\",\"spd\":\"undefined\",\"wsr\":\"{\\\"reLaunch\\\":true,\\\"relaunch\\\":true,\\\"scene\\\":1008,\\\"query\\\":{\\\"ald_share_src\\\":\\\"15257886470567357115\\\",\\\"cid\\\":\\\"5abc554dd459353b1eec0e96\\\"},\\\"path\\\":\\\"pages\\/detail\\/detail\\\"}\",\"st\":\"1526313702362\",\"usr\":\"15257886470567357115\",\"province\":\"陕西\",\"life\":\"show\",\"v\":\"6.1.2\",g\":\"{\\\"ald_share_src\\\":\\\"15257886470567357115\\\",\\\"cid\\\":\\\"5abc554dd459353b1eec0e96\\\"}\",\"sc\":\"undefined\",\"lp\":\"pages\\/detail\\/detail\",\"lua_wechat_version\":\"6.5.13.1100\",\"dr\":\"6\",\"ww\":\"360\",\"lang\":\"zh_CN\",\"at\":\"15263137022836698990\",\"country\":\"中国\",\"rq_c\":\"28\",\"city\":\"西安\",\"server_time\":1526313601.99,\"lng\":\"undefined\",\"query\":{\"ald_share_src\":\"15257886470567357115\",\"cid\":\"5abc5549353b1eec0e96\"},\"wvv\":\"android\",\"ak\":\"1cc3fe10f75202796d8cc7355651a3ad\",\"nt\":\"wifi\",\"lat\":\"undefined\",\"uu\":\"1522811064347722751\",\"path\":\"pages\\/detail\\/detail\",\"sv\":\"Android 6.0\",\"wsdk\":\"1.5.6\",\"wv\":\"6.5.13\",\"ag_ald_share_src\":\"15257886470567357115\"}"
  //    var aa= LogRecord.line2Bean(test)
  //    println(aa)
  //  }
}
