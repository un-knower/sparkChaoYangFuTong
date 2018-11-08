package com.ald.stat.module.pv

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.component.stat.PV
import com.ald.stat.log.LogRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SharePVStat extends PV{

  /**
    * 复写pv实时计算
    *
    * @param baseRedisKey redis 基key like. realTime:${datestr}:pv:00:01,realTime 为baseRedisKey
    * @param dateStr      这个日期一是从参数中获取(手动执行指定的日期)或者从 dimensionKey 中获取的日期
    * @param statTrait    具体纬度(时间、区域、年龄、客户端)的实现
    * @param rddRecords   rdd
    * @tparam C 范型
    * @tparam K 范型
    * @return 规约后的rdd
    */
  override def statIncreaseCache[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, taskId: String, statTrait: KeyTrait[C, K], rddRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(K, Long)] = {
    rddRecords.mapPartitions(
      par => {
        val csts = ArrayBuffer[(K, Long)]()
        par.foreach(record => {
          //获取基准 baseKey
          val baseKey = statTrait.getKey(record)
          csts.+=((baseKey, 1l))
        })
        csts.iterator
      }).reduceByKey((x, y) => (x + y))
  }

}
