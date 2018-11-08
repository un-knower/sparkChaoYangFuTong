package com.ald.stat.module.nu

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.component.stat.UV
import com.ald.stat.log.LogRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by admin on 2018/5/22.
  */
object NUStat extends UV {

  override val name: String = "NU"


  /**
    * 复写UV，计算新用户数，分时统计
    *
    * @param baseRedisKey
    * @param dateStr
    * @param statTrait
    * @param logRecords
    * @tparam C
    * @tparam K
    * @tparam P
    * @return
    */
  override def statIncreaseCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct().mapPartitions(par => {
      val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
      val baseKey = getKey(dateStr, baseRedisKey)
      val uidKey = getDetailKey(dateStr, baseRedisKey)
      val csts = ArrayBuffer[(P, Long)]()
      par.foreach(record => {
        val hourkey = statTrait.getKeyofDay(record).toString // ak:yyyyMMdd:uu:at
        val baseHashKey = statTrait.getBaseKey(record)

        var splits = hourkey.split(":")
        val detailkey = splits(0) + ":" + splits(1) + ":" + splits(3) + splits(4) //ak:yyyyMMdd:uu:at

        if (record.lr.ifo == "true") {
          val insertCount = resource.sadd(uidKey, detailkey) //检查插入的数量，如果是1，说明原系统没有，uid要增加数量
          if (insertCount == 1) {
            resource.hincrBy(baseKey, baseHashKey.hashKey, 1l)
            csts.+=((baseHashKey, 1))
          } else {
            csts.+=((baseHashKey, 0l))
          }
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
}
