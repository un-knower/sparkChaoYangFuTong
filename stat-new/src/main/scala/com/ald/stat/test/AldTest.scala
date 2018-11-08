//package com.ald.stat.test
//
//import com.ald.stat.component.dimension.trend.{DailyUidSubDimensionKey, HourUidSubDimensionKey}
//import com.ald.stat.kafka.hbase.SQLKafkaConsume
//import com.ald.stat.log.LogRecord
//import com.ald.stat.module.uv.UVStat
//import com.ald.stat.utils.ConfigUtils
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by root on 2018/5/19.
//  */
//object AldTest extends App {
//
//  val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
//    .setMaster(ConfigUtils.getProperty("spark.master.host"))
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.kryo.registrator", "com.ald.stat.RegisterKypoSerializer")
//
//  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(ConfigUtils.getProperty("streaming.realtime.interval").toInt))
//  val baseRedisKey = "rt:";
//  val stream = SQLKafkaConsume.getStream(ssc)
//
//  stream.foreachRDD(rdd => {
//    val logRecordRdd = rdd.mapPartitions(
//      par => {
//        val recordsRdd = ArrayBuffer[LogRecord]()
//        par.foreach(line => {
//          println(line.value())
//          recordsRdd += LogRecord.line2Bean(line.value())
//        })
//        recordsRdd.iterator
//      }
//    )
//   val uvDailyResult =UVStat.statIncreaseCache(baseRedisKey,"1526897455000",DailyUidSubDimensionKey, logRecordRdd)
//    val uvHourResult =UVStat.statIncreaseCache(baseRedisKey,"1526897455000",HourUidSubDimensionKey, logRecordRdd)
//
//    uvHourResult.foreachPartition(rows=>{
//      rows.foreach(row=>{
//        println("uvHourResult---------------------")
//        println(row) //(1548284aae6f48b854b0a96a730d4bed:20180515,1)
//      })
//    })
////    uvDailyResult.foreachPartition(rows=>{
////      rows.foreach(row=>{
////        println("uvuvuuvuvuvuuvuuvv---------------------")
////        println(row) //(1548284aae6f48b854b0a96a730d4bed:20180515,1)
////      })
////    })
////    val result = PVStat.statIncreaseCache(baseRedisKey, "1526897455000", "taskId", TimeDimensionKey, logRecordRdd)
////    //数据输入到mysql
////    result.foreachPartition(rows => {
////      val batchSql = rows.map(row => {
////        val splits = row._1.toString.split(":")
////        val app_key = splits(0)
////        val day = splits(1)
////        val hour = splits(2)
////        val visitor_count = row._2
////        val sqlInsertOrUpdate =
////          s"""
////             |insert into aldstat_trend_analysis_test(app_key,day,hour,visitor_count)values("$app_key","$day","$hour","$visitor_count")
////             |ON DUPLICATE KEY UPDATE day="$day", hour="$hour",visitor_count=visitor_count+"$visitor_count"
////        """.stripMargin
////        sqlInsertOrUpdate
////      }).toList
////      DBUtils.doBatchExecute(batchSql)
////    })
//  })
//  ssc.start()
//  ssc.awaitTermination()
//
//}
