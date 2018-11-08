package com.ald.stat.module.ct


import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension._
import com.ald.stat.component.stat.UV
import com.ald.stat.log.{LogRecord, LogRecordExtendSS}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
/**
  * Created by spark01 on 6/6/18.
  */
object CtStat extends  {

   val name = "EventCt"


  def logReRdd2LogReExtentRdd(logRecordRDD: RDD[LogRecord]): RDD[LogRecordExtendSS] = {

    logRecordRDD.mapPartitions(per => {
      var resultRDD = new ArrayBuffer[LogRecordExtendSS]()
      per.foreach(str => {

        resultRDD  ++= LogRecordExtendSS.logR2LogRExtendArray(str)

      })
      resultRDD.toIterator
    })
  }

  //gcs:def statIncreaseCache[C <: LogRecordExtendSS, K <: SubDimensionKeyExtend :ClassTag : Ordering, P <: DimensionKeyExtend :ClassTag: Ordering]
  //gcs:因为泛型在这个函数被调用的时候才会被初始化。这个K没有被用到，所以在调用的时候运行出现了错误。是因为这个K没有被初始化，所以运行出现了问题
   def statIncreaseCache[C <: LogRecordExtendSS ,P <: DimensionKeyExtend :ClassTag: Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyTraitExtend[LogRecordExtendSS,P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, Long)] = {



    logRecords.filter(item => item !=null).map(record =>
        statTrait.getKey(record)
      ). //当时所有的uid已经去掉重复的
      mapPartitions( //现在判断是否要计数
      par => {
        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
//        val baseKey = getKey(dateStr, baseRedisKey)
//        val uidKey = getDetailKey(dateStr, baseRedisKey)
        val csts = ArrayBuffer[(P, Long)]()
//        par.foreach(record => {
//          val baseHashKey = statTrait.getKey(record)
//            csts.+=((baseHashKey, 1l))
//        })


//        })

        par.foreach(baseHashKey =>{
          csts.+= ((baseHashKey, 1l))
        })
        //设置失效时间
//        resource.expire(uidKey, Daily_Stat_Age)
//        try {
////          if (resource != null)
////            resource.close()
//        }
//        catch {
//          case t: Throwable => t.printStackTrace()
//        }
        csts.iterator
      }).reduceByKey((x, y) => (x + y))
  }
}
