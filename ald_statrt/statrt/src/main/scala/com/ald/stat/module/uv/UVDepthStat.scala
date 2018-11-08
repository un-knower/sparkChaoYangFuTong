package com.ald.stat.module.uv


import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, DimensionKeyExtend, KeyParentTraitExtend, SubDimensionKey, SubDimensionKeyExtend}
import com.ald.stat.component.stat.UV
import com.ald.stat.log.{LogRecordExtendSS,LogRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
/**
  * Created by spark01 on 6/7/18.
  */
object UVDepthStat extends UV{

  override val name:String = "DepthDailyUV"

  override def getDetailKey(dateStr: String, baseKey: String): String ={
    s"$baseKey:$dateStr:UVDepth:$name"
  }

  def getSumKey(dateStr: String, baseKey: String): String ={
    s"$baseKey:$dateStr:UVDepth:sum:$name"
  }

   def statIncreaseCacheExtendUU[C <: LogRecordExtendSS, K <: SubDimensionKeyExtend :ClassTag: Ordering, P <: DimensionKeyExtend :ClassTag: Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTraitExtend[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]):  RDD[(P, (Long, (String, String)))] ={



     //gcs:将用户page页进行累加。进而来判断用户的page depth的级别
     val basehashKey = getSumKey(dateStr, baseRedisKey) //gcs:用于判断存储Redis的key值
     val logRecordWithpd = logRecords.map(record =>{
       val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
       val redisFiledKey = statTrait.getRedisFieldValue(record)
       var dpNum = resource.hincrBy(basehashKey,redisFiledKey,1L)

       //gcs:为pd进行赋值
       record.pd = if(1L == dpNum) 1.toString else if (2L == dpNum) 2.toString
       else if(3L == dpNum) 3.toString else if(4L == dpNum) 4.toString
       else if(5L <= dpNum && 10 >= dpNum) 5.toString else 6.toString

       try {
         if (resource != null)
           resource.close()
       }
       catch {
         case t: Throwable => t.printStackTrace()
       }

       statTrait.getKey(record)
     }).distinct

     logRecordWithpd.mapPartitions(par =>{

       val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
       val baseKey = getKey(dateStr, baseRedisKey) //gcs:使用Redis进行检索的key
       val uidKey = getDetailKey(dateStr, baseRedisKey)
       val csts = ArrayBuffer[(P, (Long,(String,String)))]()
       par.foreach(record =>{
         val key = statTrait.getKeyofDay(record).toString //当日只有一个.用于去重的SubDimensionKey
         val baseHashKey = statTrait.getBaseKey(record)
         val insertCount = resource.sadd(uidKey, key)

         //gcs:这个步骤很重要。hincrBy计算出来的结果在最后读取kafka的时候要读取出来。根据DimensionKey来
         if (insertCount == 1) {
           resource.hincrBy(baseKey, baseHashKey.hashKey, 1l) //gcs:将当前的
           csts.+=((baseHashKey, (1l,(baseKey,baseHashKey.hashKey))))  //gcs:在后面在读取kafka的时候，可以根据baseKey定位到Redis的key，baseHashKey.hashKey定位到field
         } else {
           csts.+=((baseHashKey, (0l,(baseKey,baseHashKey.hashKey))))
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
     }).reduceByKey((x,y) =>{
       (x._1+y._1,(x._2._1,x._2._2))   //gcs:(count,(baseKey,baseHashKey.hash))
     }).mapPartitions(per =>{

       val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
       var csts = new ArrayBuffer[(P, (Long, (String, String)))]()
       per.foreach(str =>{ //gcs:(P, (Long, (String, String)))
         var baseKeyRedisCount = s"${str._2._2._1}:depth" //gcs:bashKey:depth
         var baseKeyFieldUU = s"${str._2._2._2}:uu"  //gcs:
//         var baseKeyFieldAT = s"${str._2._2._2}:at"
         var uuCount = resource.hincrBy(baseKeyRedisCount,baseKeyFieldUU,str._2._1) //gcs:把当前
         csts += ((str._1,(uuCount,(str._2._2._1,str._2._2._2))))
       })

       try {
         if (resource != null)
           resource.close()
       }
       catch {
         case t: Throwable => t.printStackTrace()
       }

       csts.iterator
     })
  }



  def statIncreaseCacheExtendAT[C <: LogRecordExtendSS, K <: SubDimensionKeyExtend :ClassTag: Ordering, P <: DimensionKeyExtend :ClassTag: Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTraitExtend[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]):  RDD[(P, (Long, (String, String)))] ={


    val basehashKey = getSumKey(dateStr, baseRedisKey)
    val logRecordWithpd = logRecords.map(record =>{
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val redisFiledKey = statTrait.getRedisFieldValue(record)
      var dpNum = resource.hincrBy(basehashKey,redisFiledKey,1L)


      //gcs:为pd进行赋值
      record.pd = if(1L == dpNum) 1.toString
      else if (2L == dpNum) 2.toString
      else if(3L == dpNum) 3.toString
      else if(4L == dpNum) 4.toString
      else if(5L <= dpNum && 10 >= dpNum) 5.toString
      else 6.toString

      try {
        if (resource != null)
          resource.close()
      }
      catch {
        case t: Throwable => t.printStackTrace()
      }

      statTrait.getKey(record)
    }).distinct

    logRecordWithpd.mapPartitions(par =>{

      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val baseKey = getKey(dateStr, baseRedisKey) //gcs:使用Redis进行检索的key
      val uidKey = getDetailKey(dateStr, baseRedisKey)
      val csts = ArrayBuffer[(P, (Long,(String,String)))]()
      par.foreach(record =>{
        val key = statTrait.getKeyofDay(record).toString //ges:SubDimensionKey，用来去重。当日只有一个.
        val baseHashKey = statTrait.getBaseKey(record)
        val insertCount = resource.sadd(uidKey, key)

        //gcs:这个步骤很重要。hincrBy计算出来的结果在最后读取kafka的时候要读取出来。根据DimensionKey来
        if (insertCount == 1) {
          resource.hincrBy(baseKey, baseHashKey.hashKey, 1l)
          csts.+=((baseHashKey, (1l,(baseKey,baseHashKey.hashKey))))  //gcs:在后面在读取kafka的时候，可以根据baseKey定位到Redis的key，baseHashKey.hashKey定位到field
        } else {
          csts.+=((baseHashKey, (0l,(baseKey,baseHashKey.hashKey))))
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
    }).reduceByKey((x,y) =>{
      (x._1+y._1,(x._2._1,x._2._2))   //gcs:(count,(baseKey,baseHashKey.hash))
    }).mapPartitions(per =>{

      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      var csts = new ArrayBuffer[(P, (Long, (String, String)))]()
      per.foreach(str =>{ //gcs:(P, (Long, (String, String)))
        var baseKeyRedisCount = s"${str._2._2._1}:depth" //gcs:bashKey:depth
      var baseKeyFieldUU = s"${str._2._2._2}:at"
        //         var baseKeyFieldAT = s"${str._2._2._2}:at"

        var uuCount = resource.hincrBy(baseKeyRedisCount,baseKeyFieldUU,str._2._1)
        csts += ((str._1,(uuCount,(str._2._2._1,str._2._2._2))))
      })

      try {
        if (resource != null)
          resource.close()
      }
      catch {
        case t: Throwable => t.printStackTrace()
      }

      csts.iterator
    })
  }
}
