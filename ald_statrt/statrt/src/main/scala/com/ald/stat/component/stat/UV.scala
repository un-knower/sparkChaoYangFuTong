package com.ald.stat.component.stat

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait UV extends StatBase {

  val name = "UV"

  def stat[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))). //去重
      map(record => (statTrait.getBaseKey(record), 1l)).reduceByKey((x, y) => (x + y)) //去掉UID算和
  }

  /**
    * Cache中需要存入当日UID，如果发现有相同的UID应该要算0，所以原先方法是有问题的
    *
    * @param dateStr
    * @param statTrait
    * @param logRecords
    * @tparam C
    * @tparam K
    * @tparam P
    */
  def statIncreaseCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))). //当时所有的uid已经去掉重复的
      mapPartitions( //现在判断是否要计数
      par => {
        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
        val uidKey = getDetailKey(dateStr, baseRedisKey)
        val csts = ArrayBuffer[(P, Long)]()
        import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
        val cacheSet: Set[Int] = mutable.HashSet()

        par.foreach(record => {
          val key = statTrait.getKeyofDay(record).toString //当日只有一个
          val baseHashKey = statTrait.getBaseKey(record)
          val unionKey = (uidKey + key).hashCode
          val uvKey = uidKey + ":" + "uv"
          var uv = 0l
          //获取原uv
          val oUvStr = resource.get(uvKey)
          if (StringUtils.isNotBlank(oUvStr)) {
            try {
              uv = oUvStr.toLong
            } catch {
              case e: Throwable => {
                uv = 0l
                e.printStackTrace()
              }
            }
          }
          //本地set直接判断
          if (!cacheSet.contains(unionKey)) {
            if (resource.sadd(uidKey, key) == 1) {
              cacheSet.add(unionKey)
              uv = resource.incr(uvKey)
            } else {
              cacheSet.add(unionKey)
            }
          }
          resource.expire(uvKey, Daily_Stat_Age)
          csts.+=((baseHashKey, uv))
        })
        //设置失效时间
        resource.expire(uidKey, Daily_Stat_Age)
        try {
          if (resource != null)
            resource.close()
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }
        csts.iterator
      }).reduceByKey((x, y) => (Math.max(x, y)))
  }

  /**
    * Cache中需要存入当日UID，如果发现有相同的UID应该要算0，所以原先方法是有问题的
    *
    * @param dateStr
    * @param statTrait
    * @param logRecords
    * @tparam C
    * @tparam K
    * @tparam P (k,pv,uv)
    */
  def statIncreaseCacheWithPV[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, (Long, Long))] = {
    logRecords.map(record => (statTrait.getKey(record))). //当时所有的uid已经去掉重复的
      mapPartitions( //现在判断是否要计数
      par => {
        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
        val uidKey = getDetailKey(dateStr, baseRedisKey)
        val csts = ArrayBuffer[(P, (Long, Long))]()
        import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
        val cacheSet: Set[Int] = Set()

        par.foreach(record => {
          val key = statTrait.getKeyofDay(record).toString //当日只有一个
          val baseHashKey = statTrait.getBaseKey(record)
          val unionKey = (uidKey + key).hashCode
          val pvKey = uidKey + ":" + "pv"
          val uvKey = uidKey + ":" + "uv"
          //本地set直接判断
          val pv = resource.incr(pvKey)
          var uv = 0l
          //获取原uv
          val oUvStr = resource.get(uvKey)
          if (StringUtils.isNotBlank(oUvStr)) {
            try {
              uv = oUvStr.toLong
            } catch {
              case e: Throwable => {
                uv = 0l
                e.printStackTrace()
              }
            }
          }
          if (!cacheSet.contains(unionKey)) {
            if (resource.sadd(uidKey, key) == 1) {
              cacheSet.add(unionKey)
              uv = resource.incr(uvKey)
            } else {
              cacheSet.add(unionKey)
            }
          }
          resource.expire(pvKey, Daily_Stat_Age)
          resource.expire(uvKey, Daily_Stat_Age)
          csts.+=((baseHashKey, (pv, uv)))
        })
        //设置失效时间
        resource.expire(uidKey, Daily_Stat_Age)
        try {
          if (resource != null)
            resource.close()
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }
        csts.iterator
        //规约最后一次的记录
      }).reduceByKey((a, b) => (Math.max(a._1, b._1), Math.max(a._2, b._2)))
  }

  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:$name"
  }

  def getDetailKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:UID:$name"
  }
}
