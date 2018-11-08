package com.ald.stat.component.stat

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait PV extends StatBase {

  //  lazy val redisCache = CacheClientFactory.getInstances(Redis_Prefix).asInstanceOf[ClientRedisCache]
  val name = "PV"

  def stat[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering](statTrait: KeyTrait[C, K], logRecords: RDD[C]): RDD[(K, Long)] = {

    logRecords.filter(record => record.ev == "page").map(record => (statTrait.getKey(record), 1l)).reduceByKey((x, y) => (x + y))
  }

  def statIncreaseCache[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering](baseRedisKey: String, dateStr: String, taskId: String, statTrait: KeyTrait[C, K], rddRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(K, Long)] = {
    rddRecords.mapPartitions(
      par => {
        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
        val csts = ArrayBuffer[(K, Long)]()
        val redisKey = getKey(dateStr, baseRedisKey)
        par.foreach(record => {
          val baseKey = statTrait.getKey(record)
          if (record.ev == "page") {
            csts.+=((baseKey, 0l))
          } else {
            resource.hincrBy(redisKey, baseKey.hashKey, 1l)
            csts.+=((baseKey, 1l))
          }
        })
        resource.expire(redisKey, Daily_Stat_Age)
        try {
          if (resource != null)
            resource.close()
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }
        csts.iterator
      }).reduceByKey((x, y) => (x + y))
  }

  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:$name"
  }
}
