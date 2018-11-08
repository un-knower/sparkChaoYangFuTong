//package com.ald.stat.test
//
//import com.ald.stat.component.dimension.phoneModel._
//import com.ald.stat.job.AnalysisTrend.redisCache
//import com.ald.stat.kafka.hbase.{SQLKafkaConsume, SQLKafkaProducer}
//import com.ald.stat.log.LogRecord
//import com.ald.stat.module.pv.PVStat
//import com.ald.stat.module.session.{SessionBaseImpl, SessionStatImpl}
//import com.ald.stat.module.uv.UVStat
//import com.ald.stat.utils.DBUtils.getConnection
//import com.ald.stat.utils.{ConfigUtils, DBUtils}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * 机型分析
//  * 机型维度测试
//  * Created by admin on 2018/5/24.
//  */
//object ModelTest extends App{
//  val baseRedisKey = "rt:";
//  val dateStr = "20180101"
//  val taskId = "taskId"
//
//  val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
//    .setMaster(ConfigUtils.getProperty("spark.master.host"))
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
//
//  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))
//  val stream = SQLKafkaConsume.getStream(ssc)
//
//
//  val modelRedisKey = "rt:model"
//  val clientRedisCache = redisCache.getResource
//  if (!clientRedisCache.exists(modelRedisKey)) {
//    val conn = getConnection();
//    val statement = conn.createStatement()
//    //uname:型号(iPhone 5s (GSM)<iPhone6,1>)   对应于log中的pm字段。
//    //name:机型 (iPhone 5s)
//    //brand:品牌(iPhone)
//    val rs = statement.executeQuery("select uname,name from phone_model")
//    while (rs.next()) {
//      val uname = rs.getInt(1)
//      val name = rs.getInt(2)
//      if (uname != null && name != null)
//        clientRedisCache.hset(modelRedisKey, uname.toString, name.toString)
//    }
//  }
//
//  stream.foreachRDD(rdd => {
//
//    val logRecordRdd = rdd.mapPartitions(
//      par => {
//        val recordsRdd = ArrayBuffer[LogRecord]()
//        val clientRedisCache = redisCache.getResource
//        par.foreach(line => {
//          println(line.value())
//          val logRecord = LogRecord.line2Bean(line.value())
//          if (logRecord.pm != null && logRecord.pm != "") {
//            //pm:对应于日志中的手机型号  对应于phone_model表中的uname字段。
//            val model = clientRedisCache.hget(modelRedisKey, logRecord.pm)  //机型
//            if (model != "nil"){
//              logRecord.model = model
//            }else{
//              logRecord.model = "未知"  //如果不存在则把机型值赋值为"未知"
//            }
//            recordsRdd += logRecord
//          }
//        })
//        recordsRdd.iterator
//      }
//    )
//    //保存
//    logRecordRdd.cache();
//
//    val result = SessionStatImpl.stat(logRecordRdd, DailyModelSessionSubDimensionKey, SessionBaseImpl)
//    //session处理
//    val sessionRDD = SessionStatImpl.doCache(baseRedisKey, taskId, dateStr, result, DailyModelSessionSubDimensionKey)
//    //uv
//    val uvRDD = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyModelUidSubDimensionKey, logRecordRdd)
//    //open count
//    val opRDD = UVStat.statIncreaseCache(baseRedisKey, dateStr, DailyModelUidSubDimensionKey, logRecordRdd)
//    //pv
//    val pvRDD = PVStat.statIncreaseCache(baseRedisKey, dateStr, taskId, DailyModelDimensionKey, logRecordRdd)
//
//    //最终结果集
//    val finalRDD = pvRDD.join(uvRDD).join(opRDD).join(sessionRDD)
//
//    finalRDD.foreachPartition(par => {
//      val batchSql = par.map(row => {
//
//        //从DimensionKey中获取
//        val splits = row._1.toString.split(":")
//        val app_key = splits(0)
//        val day = splits(1)
//        val phone_model = splits(2)
//
//        val sessionSum = row._2._2
//
//        val new_comer_count = sessionSum.newUserCount
//        val visitor_count = row._2._1._1._2
//        val open_count = row._2._1._2
//        val total_page_count = row._2._1._1._1
//        var avg_stay_time = 0f
//        if (visitor_count != 0) {
//          avg_stay_time = sessionSum.sessionDurationSum / visitor_count
//        }
//        var secondary_avg_stay_time = 0f
//        if (total_page_count != 0) {
//          secondary_avg_stay_time = sessionSum.sessionDurationSum / total_page_count
//        }
//        val total_stay_time = sessionSum.sessionDurationSum
//        val one_page_count = sessionSum.onePageofSession
//        //跳出率
//        var bounce_rate = 0f
//        if (open_count != 0) {
//          bounce_rate = one_page_count.toFloat / open_count.toFloat
//        }
//        val sqlInsertOrUpdate =
//          s"""
//             |insert into ald_device_statistics
//             |(
//             |    app_key,
//             |    day,
//             |    phone_model,
//             |    new_comer_count,
//             |    visitor_count,
//             |    open_count,
//             |    total_page_count,
//             |    avg_stay_time,
//             |    secondary_avg_stay_time,
//             |    total_stay_time,
//             |    one_page_count,
//             |    bounce_rate,
//             |    update_at
//             |)
//             |values(
//             |    "$app_key",
//             |    "$day",
//             |    "$phone_model",
//             |    "$new_comer_count",
//             |    "$visitor_count",
//             |    "$open_count",
//             |    "$total_page_count",
//             |    "$avg_stay_time",
//             |    "$secondary_avg_stay_time",
//             |    "$total_stay_time",
//             |    "$one_page_count",
//             |    "$bounce_rate",
//             |    now()
//             |)
//             |on DUPLICATE KEY UPDATE
//             |app_key="$app_key",
//             |day="$day",
//             |hour="$phone_model",
//             |new_comer_count=new_comer_count+"$new_comer_count",
//             |visitor_count=visitor_count+"$visitor_count",
//             |open_count=open_count+"$open_count",
//             |total_stay_time=total_stay_time+"$total_stay_time",
//             |total_page_count=total_page_count+"$total_page_count",
//             |avg_stay_time=ifnull(round(total_stay_time/visitor_count,2),0),
//             |secondary_avg_stay_time=ifnull(round(total_stay_time/open_count,2),0),
//             |one_page_count=one_page_count+"$one_page_count",
//             |bounce_rate=ifnull(round(one_page_count/open_count,2),0),
//             |update_at=now()
//            """.stripMargin
//
//        sqlInsertOrUpdate
//      }).toList
//      //数据进入kafka
//      SQLKafkaProducer.write("")
//      //直接执行mysql
//      DBUtils.doBatchExecute(batchSql)
//    })
//  })
//  ssc.start()
//  ssc.awaitTermination()
//}
