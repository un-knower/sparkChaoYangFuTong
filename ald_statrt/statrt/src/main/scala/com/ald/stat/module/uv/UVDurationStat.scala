package com.ald.stat.module.uv


import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, DimensionKeyExtend, KeyParentTraitExtend, SubDimensionKey, SubDimensionKeyExtend}
import com.ald.stat.component.stat.UV
import com.ald.stat.log.LogRecordExtendSS
import com.ald.stat.module.uv.UVDepthStat.{getDetailKey, getKey, name}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by spark01 on 6/8/18.
  */
object UVDurationStat extends UV{

  override val name = "durationDailyUV"

  override def getDetailKey(dateStr: String, baseKey: String): String ={
    s"$baseKey:$dateStr:UVDuration:$name"
  }

  def getSumKey(dateStr: String, baseKey: String): String ={
    s"$baseKey:$dateStr:UVDuration:sum:$name"
  }


  def statIncreaseCacheExtendUU[C <: LogRecordExtendSS, K <: SubDimensionKeyExtend :ClassTag: Ordering, P <: DimensionKeyExtend :ClassTag: Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTraitExtend[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]):  RDD[(P, (Long, (String, String)))] ={



    val basehashKey = getSumKey(dateStr, baseRedisKey)

    val loRecord2DimensionKey = logRecords.map(record =>{
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val redisFieldKey = statTrait.getRedisFieldValue(record)
      var redisDr = resource.hget(basehashKey,redisFieldKey)
      if (!StringUtils.isNotBlank(redisDr)){ //gcs:提取出来的nil可能为
        redisDr = 0.toString
      }
      val recordDr = record.dr.toLong

      //gcs:当当前的这条recordDr 大于 存储在redis当中的dr的时候，才会进行.这个逻辑的目的就是取出比较大的Dr
      if (recordDr > redisDr.toLong){

        resource.hset(basehashKey,redisFieldKey,recordDr.toString)
        record.dd = if (recordDr >=0 && recordDr <= 2 ) 1.toString
        else if (recordDr >=3 && recordDr <= 5) 2.toString
        else if (recordDr >=6 && recordDr <=10) 3.toString
        else if (recordDr >= 11 && recordDr <= 20) 4.toString
        else if (recordDr >= 21 && recordDr <= 30) 5.toString
        else if (recordDr >= 31 && recordDr <=50) 6.toString
        else if(recordDr >=51 && recordDr <= 100) 7.toString
        else 8.toString

      }
      else {

        var drOfRedis = redisDr.toLong
        record.dd = if (drOfRedis >=0 && drOfRedis <=2) 1.toString
        else if(drOfRedis >=3 && drOfRedis <=5) 2.toString
        else if(drOfRedis >=6 && drOfRedis <=10) 3.toString
        else if(drOfRedis >=11 && drOfRedis <=20) 4.toString
        else if(drOfRedis >=21 && drOfRedis <= 30) 5.toString
        else if(drOfRedis >=31 && drOfRedis <=50) 6.toString
        else if(drOfRedis >=51 && drOfRedis <=100) 7.toString
        else 8.toString
      }

      try {
        if (resource != null)
          resource.close()
      }
      catch {
        case t: Throwable => t.printStackTrace()
      }

      statTrait.getKey(record)
    }).distinct

    //gcs:进行reduceByKey的操作
    loRecord2DimensionKey.mapPartitions(par =>{
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val baseKey = getKey(dateStr, baseRedisKey) //gcs:使用Redis进行检索的key
      val uidKey = getDetailKey(dateStr, baseRedisKey)
      val csts = ArrayBuffer[(P, (Long,(String,String)))]()
      par.foreach(record =>{
        val key = statTrait.getKeyofDay(record).toString //当日只有一个.用于去重的SubDimensionKey
        val baseHashKey = statTrait.getBaseKey(record)
        val insertCount = resource.sadd(uidKey, key)

        if (insertCount == 1) {
          resource.hincrBy(baseKey, baseHashKey.hashKey, 1l) //gcs:将当前的
          csts.+=((baseHashKey, (1l,(baseKey,baseHashKey.hashKey))))  //gcs:在后面在读取kafka的时候，可以根据baseKey定位到Redis的key，baseHashKey.hashKey定位到field
        }
        else{
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
    }).reduceByKey((x,y) => {
      (x._1 + y._1, (x._2._1, x._2._2)) //gcs:(count,(baseKey,baseHashKey.hash))
    }).mapPartitions(per =>{
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      var csts = new ArrayBuffer[(P, (Long, (String, String)))]()
      per.foreach(str =>{ //gcs:(P, (Long, (String, String)))
        var baseKeyRedisCount = s"${str._2._2._1}:duration" //gcs:bashKey:depth
      var baseKeyFieldUU = s"${str._2._2._2}:uu"  //gcs:
      //         var baseKeyFieldAT = s"${str._2._2._2}:at"
      var uuCount = resource.hincrBy(baseKeyRedisCount,baseKeyFieldUU,str._2._1) //gcs:把当前批次的结果写到这个批次到之前的所有的结果当中
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



  def statIncreaseCacheExtendAT[C <: LogRecordExtendSS, K <: SubDimensionKeyExtend :ClassTag: Ordering, P <: DimensionKeyExtend  :ClassTag: Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTraitExtend[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]):  RDD[(P, (Long, (String, String)))] ={



    val basehashKey = getSumKey(dateStr, baseRedisKey)

    val loRecord2DimensionKey = logRecords.map(record =>{
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val redisFieldKey = statTrait.getRedisFieldValue(record)
      var redisDr = resource.hget(basehashKey,redisFieldKey)

      if (!StringUtils.isNotBlank(redisDr)){ //gcs:提取出来的nil可能为
        redisDr = 0.toString
      }

      val recordDr = record.dr.toLong
      //gcs:当当前的这条recordDr 大于 存储在redis当中的dr的时候，才会进行
      if (recordDr > redisDr.toLong){
        resource.hset(basehashKey,redisFieldKey,recordDr.toString)
        record.dd = if (recordDr >=0 && recordDr <= 2 ) 1.toString
        else if (recordDr >=3 && recordDr <= 5) 2.toString
        else if (recordDr >=6 && recordDr <=10) 3.toString
        else if (recordDr >= 11 && recordDr <= 20) 4.toString
        else if (recordDr >= 21 && recordDr <= 30) 5.toString
        else if (recordDr >= 31 && recordDr <=50) 6.toString
        else if(recordDr >=51 && recordDr <= 100) 7.toString
        else 8.toString
      }else {
        var drOfRedis =redisDr.toLong
        record.dd = if (drOfRedis >=0 && drOfRedis <=2) 1.toString
        else if(drOfRedis >=3 && drOfRedis <=5) 2.toString
        else if(drOfRedis >=6 && drOfRedis <=10) 3.toString
        else if(drOfRedis >=11 && drOfRedis <=20) 4.toString
        else if(drOfRedis >=21 && drOfRedis <= 30) 5.toString
        else if(drOfRedis >=31 && drOfRedis <=50) 6.toString
        else if(drOfRedis >=51 && drOfRedis <=100) 7.toString
        else 8.toString
      }

      try {
        if (resource != null)
          resource.close()
      }
      catch {
        case t: Throwable => t.printStackTrace()
      }

      statTrait.getKey(record)
    }).distinct

    //gcs:进行reduceByKey的操作
    loRecord2DimensionKey.mapPartitions(par =>{
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val baseKey = getKey(dateStr, baseRedisKey) //gcs:使用Redis进行检索的key
      val uidKey = getDetailKey(dateStr, baseRedisKey)
      val csts = ArrayBuffer[(P, (Long,(String,String)))]()
      par.foreach(record =>{
        val key = statTrait.getKeyofDay(record).toString //当日只有一个.用于去重的SubDimensionKey
        val baseHashKey = statTrait.getBaseKey(record)
        val insertCount = resource.sadd(uidKey, key)

        if (insertCount == 1) {
          resource.hincrBy(baseKey, baseHashKey.hashKey, 1l) //gcs:将当前的
          csts.+=((baseHashKey, (1l,(baseKey,baseHashKey.hashKey))))  //gcs:在后面在读取kafka的时候，可以根据baseKey定位到Redis的key，baseHashKey.hashKey定位到field
        }
        else{
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
    }).reduceByKey((x,y) => {
      (x._1 + y._1, (x._2._1, x._2._2)) //gcs:(count,(baseKey,baseHashKey.hash))
    }).mapPartitions(per =>{
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      var csts = new ArrayBuffer[(P, (Long, (String, String)))]()
      per.foreach(str =>{ //gcs:(P, (Long, (String, String)))
        var baseKeyRedisCount = s"${str._2._2._1}:duration" //gcs:bashKey:depth
      var baseKeyFieldAt = s"${str._2._2._2}:at"  //gcs:
      //         var baseKeyFieldAT = s"${str._2._2._2}:at"
      var atCount = resource.hincrBy(baseKeyRedisCount,baseKeyFieldAt,str._2._1) //gcs:把当前批次的结果写到这个批次到之前的所有的结果当中
        csts += ((str._1,(atCount,(str._2._2._1,str._2._2._2))))
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
