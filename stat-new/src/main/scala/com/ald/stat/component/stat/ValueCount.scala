package com.ald.stat.component.stat

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension._
import com.ald.stat.log.LogRecord
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait ValueCount extends StatBase {
  lazy val redisCache = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache]
  val name = "VC"

  def stat[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], valueF : C=> Long): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getBaseKey(statTrait.getKey(record)), valueF(record))).reduceByKey((x, y) => (x + y)) //去掉UID算和
  }


  def statIncreaseCache[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering](baseRedisKey: String, taskId: String, dateStr: String, statTrait: KeyTrait[C, K], rddRecords: RDD[C], valueF : C=> Long): RDD[(K, Long)] = {
    rddRecords.mapPartitions(
      par => {
        val csts = ArrayBuffer[(K, Long)]()
        val resource = redisCache.getResource
        val redisKey = getKey(dateStr, baseRedisKey)
//        val testBaseKey = redisKey + ":" + taskId
        par.foreach(record => {
          val baseKey = statTrait.getKey(record)
          val incr = valueF(record)
//          resource.hincrBy(testBaseKey, baseKey.key,incr)
//          resource.hincrBy(testBaseKey, "count", 1l)
          resource.hincrBy(redisKey, baseKey.hashKey, incr)
          csts.+=((baseKey, incr))
        })
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
    s"$baseKey:$dateStr$name"
  }
}
