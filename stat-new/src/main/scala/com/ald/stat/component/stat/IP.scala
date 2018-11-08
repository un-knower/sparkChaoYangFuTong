package com.ald.stat.component.stat

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait IP extends StatBase {
  lazy val redisCache = CacheClientFactory.getInstances(Redis_Prefix).asInstanceOf[ClientRedisCache]
  val name = "IP"

  def stat[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct(). //去重
      map(record => (statTrait.getBaseKey(record), 1l)).reduceByKey((x, y) => (x + y)) //去掉UID算和
  }


  def statIncreaseCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, taskId: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct(). //当时所有的ip已经去掉重复的
      mapPartitions( //现在判断是否要计数
      par => {
        val resource = redisCache.getResource
        val baseKey = getKey(dateStr, baseRedisKey)
        val uidKey = getDetailKey(dateStr, baseRedisKey)
        val csts = ArrayBuffer[(P, Long)]()
        par.foreach(record => {
          val key = statTrait.getKeyofDay(record).toString //当日只有一个
          val baseHashKey = statTrait.getBaseKey(record)
          val insertCount = resource.sadd(uidKey, key) //检查插入的数量，如果是1，说明原系统没有，uid要增加数量
          if (insertCount == 1) {
            resource.hincrBy(baseKey, baseHashKey.hashKey, 1l)
            csts.+=((baseHashKey, insertCount))
          }
          else {
            csts.+=((baseHashKey, 0l))
          }
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

  def getDetailKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:IP:$dateStr$name"
  }
}
