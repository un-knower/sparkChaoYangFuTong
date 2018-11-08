//package com.ald.stat.module.openCount
//
//import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
//import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
//import com.ald.stat.component.stat.StatBase
//import com.ald.stat.log.LogRecord
//import com.ald.stat.utils.ComputeTimeUtils
//import org.apache.spark.rdd.RDD
//
//import scala.collection.mutable.ArrayBuffer
//import scala.reflect.ClassTag
//
///**
//  * Created by spark01 on 18-5-22.
//  */
//object OpenCount2 extends StatBase{
//
//  lazy val redisCache = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache]
//  val name = "OpenCount"
//  /**
//    * <b>author:</b> gcs <br>
//    * <b>data:</b> 18-5-22 <br>
//    * <b>description:</b><br>
//    * <b>param:</b><br>
//    *   statTrait: KeyTrait[C, K] ;要传进去 OpenCountOfDailyDimensionKey
//    * <b>return:</b><br>
//    */
//  def opencountOfDailyIncreaseCache[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering](baseRedisKey: String, dateStr: String, taskId: String, statTrait: KeyTrait[C, K], rddRecords: RDD[C]): RDD[(K, Long)] = {
//
//
//
//    rddRecords.map(record => (statTrait.getKey(record))).distinct.mapPartitions(
//      par => {
//        val csts = ArrayBuffer[(K, Long)]()
//        val resource = redisCache.getResource
//        val redisKey = getKey(dateStr, baseRedisKey)
//
//        par.foreach(record => {
//
//
//
//          val baseKey = record.key //gcs:获得一天的baseKey和at字段
//          if ("app" == record.lr.ev) { //gcs:当ev不等于app的时候，不适用适用OpenCount的开发
//
//
//            var insertCount = resource.sadd(redisKey,baseKey.toString)  //gcs:将session不相同的记录插入到当前的这个rediskey当中  basek的结构ak:yyyy:at
//
//            if (1 == insertCount){
//              csts.+=((record, 1l))
//            }
//            else {
//              csts.+=((record, 0l))
//            }
//
//          } else {
//            csts.+=((record, 0l))
//          }
//        })
//        try {
//          if (resource != null)
//            resource.close() //gcs:Redis关闭，还没继续使用吗？？
//        }
//        catch {
//          case t: Throwable => t.printStackTrace()
//        }
//        csts.iterator
//      }).reduceByKey((x, y) => (x + y)) //gcs:先对整理完的数据进行去重的操作。之后再进行ReduceByKey的操作
//
//    //gcs:(ak+yyyy+at,count)
//  }
//
//
//  /**
//    * <b>author:</b> gcs <br>
//    * <b>data:</b> 18-5-22 <br>
//    * <b>description:</b><br>
//    *
//    * <b>param:</b><br>
//    *   statTrait: KeyTrait[C, K] ;要传进去 OpenCountOfDailyDimensionKey 需要调用OpenCountOfHourDimensionKey
//    *   KeyParentTrait
//    * <b>return:</b><br>
//    */
//  def openCountOfHourIncreaseCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering](baseRedisKey: String, dateStr: String, taskId: String, statTrait: KeyParentTrait[C, K, K], rddRecords: RDD[C]): RDD[(K, Long)] = {
//
//
//    rddRecords.map(record => (statTrait.getKey(record))).distinct.mapPartitions(
//      par => {
//        val csts = ArrayBuffer[(K, Long)]()
//        val resource = redisCache.getResource
//        val redisDayKey = getKey(dateStr, baseRedisKey) //gcs:获得一天的redisDayKey
//
//
//        par.foreach(record => {
//          val redisKey = getHourKey(dateStr, baseRedisKey,record)  //gcs: 获得小时的hourKey进行操作的key s"${baseKey}:${dateStr}:$hour:${name}:"
//
//          val baseDayKey =statTrait.getKeyofDay(record)
//          if ("app" != record.lr.ev) {
//            csts.+=((record, 0l))
//          } else if (baseDayKey !=""){
//            val insertCount = resource.sadd(redisDayKey,baseDayKey)  //gcs:将session不相同的记录插入到当前的这个rediskey当中  ak:yyyy:hour:at
//            if(insertCount == 1){ //gcs:当判断一天的数据中还没有这个session的值
//              csts.+=((record, 1l))
//              resource.sadd(redisKey,record.toString)
//            }
//          }
//        })
//        try {
//          if (resource != null)
//            resource.close()
//        }
//        catch {
//          case t: Throwable => t.printStackTrace()
//        }
//        csts.iterator
//      })reduceByKey((x, y) => (x + y)) //gcs:先对整理完的数据进行去重的操作。之后再进行ReduceByKey的操作
//
//    //gcs:(ak+yyyy+hout+at,count)
//  }
//
//
//  def getKey(dateStr: String, baseKey: String): String = {
//    s"$baseKey:$dateStr:$name"
//  }
//
//  def getHourKey[C <: LogRecord](dateStr: String, baseKey: String,logRecord:C):String={
//
//    val hour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)._2
//
//    s"${baseKey}:${dateStr}:$hour:${name}:"
//  }
//
//}
