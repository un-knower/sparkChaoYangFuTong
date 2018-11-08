package com.ald.stat.module.uv

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.component.stat.UV
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by root on 2018/5/22.
  */
object UVStat extends UV {

  override val name: String = "DailyUV"

  override def statIncreaseCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct().mapPartitions(par => {

      val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
      val uidKey = getDetailKey(dateStr, baseRedisKey)
      val csts = ArrayBuffer[(P, Long)]()
      par.foreach(record => {
        val key = statTrait.getKeyofDay(record).toString //当日只有一个
        val baseHashKey = statTrait.getBaseKey(record)
        csts.+=((baseHashKey, 0l))
        val insertCount = resource.sadd(uidKey, key) //检查插入的数量，如果是1，说明原系统没有，uid要增加数量
        if (insertCount == 1) {
          csts.+=((baseHashKey, 1))
        } else {
          csts.+=((baseHashKey, 0l))
        }
      })
      //设置失效时间
      resource.expireAt(uidKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
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
}
