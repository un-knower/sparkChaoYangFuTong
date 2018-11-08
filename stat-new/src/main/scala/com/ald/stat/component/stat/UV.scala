package com.ald.stat.component.stat

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}
import com.ald.stat.utils.DBUtils.use
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
    * 补偿计算PV
    * @param baseRedisKey
    * @param dateStr
    * @param statTrait
    * @param logRecords
    * @param redisPrefix
    * @tparam C
    * @tparam K
    * @return
    */
  def statPatchPV[C <: LogRecord, K <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String,statTrait: KeyTrait[C, K], logRecords: RDD[C],redisPrefix: Broadcast[String]): RDD[(K, Long)] = {
    //.filter(record => record.ev == "page")
    logRecords.map(record =>(statTrait.getKey(record), 1l)).reduceByKey((x, y) => (x + y)).mapPartitions(par=>{
      val arrayBuffer = ArrayBuffer[(K, Long)]()
      val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
      use(redisCache.getResource) { resource =>
        par.foreach(record =>{
          val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
          resource.set(pvKey,record._2.toString)//替换redis中的PV
          resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          arrayBuffer.+=((record._1,record._2))
        })
      }
      arrayBuffer.iterator
    })
  }

  /**
    * 补偿计算UV
    * @param baseRedisKey
    * @param dateStr
    * @param statTrait
    * @param logRecords
    * @param redisPrefix
    * @tparam C
    * @tparam K
    * @tparam P
    * @return
    */
  def statPatchUV[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String,statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C],redisPrefix: Broadcast[String]): RDD[(P, Long)] = {
    logRecords.map(record => (statTrait.getKey(record))).distinct()
      .mapPartitions(par=>{
        val csts = ArrayBuffer[(P,Long)]()
        val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
        use(redisCache.getResource) {
          resource => {
            par.foreach(record=>{
              var uidKey: String = dateStr
              uidKey = getDetailKey(dateStr, baseRedisKey)
              val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
              val expand_Key = uidKey + ":" + key //展开后的key
              val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
              val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key
              if(resource.exists(final_key) == false){//存在返回1，不存在返回0
                resource.set(final_key,"1")//不存在，则添加，并赋值为1
                resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime)//设置为次日凌晨2点过期
              }
              csts.+=((statTrait.getBaseKey(record), 1l))
            })
          }
        }
        csts.iterator
      }).reduceByKey((x, y) => (x + y)).mapPartitions(par=>{
        val arrayBuffer = ArrayBuffer[(P, Long)]()
        val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
        use(redisCache.getResource) { resource =>
          par.foreach(record =>{
            val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
            resource.set(uvKey,record._2.toString)//替换redis中的UV
            resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
            arrayBuffer.+=((record._1,record._2))
          })
        }
        arrayBuffer.iterator
    })
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
          resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          csts.+=((baseHashKey, uv))
        })
        //设置失效时间
        resource.expireAt(uidKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
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

  def getDetailKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:UID:$name"
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

    logRecords.map(record => (statTrait.getKey(record))).mapPartitions(par =>{
        val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
        val csts = ArrayBuffer[(P, (Long, Long))]()
        try{
          use(redisCache.getResource) {
            resource => {
              import scala.collection.mutable.Set // 可以在任何地方引入 可变集合
              val cacheSet: Set[Int] = Set()
              var uidKey: String = dateStr
              par.foreach(record => {
                uidKey = getDetailKey(dateStr, baseRedisKey)
                val key = statTrait.getKeyofDay(record).toString //获取subDimensionKey的hash值
                val baseKey = statTrait.getBaseKey(record) //获取dimensionKey
                val unionKey = (uidKey + key).hashCode
                var tuple = (1l, 0l) //创建一个元组（pv,uv）,pv的默认值就是1
                if (!cacheSet.contains(unionKey)) {//本地set直接判断，用于当前批次内的快速去重
                  val expand_Key = uidKey + ":" + key //展开后的key
                  val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
                  val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key
                  if(resource.exists(final_key) == false){//存在返回1，不存在返回0
                    resource.set(final_key,"1")//不存在，则添加，并赋值为1
                    resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime)//设置为次日凌晨2点过期
                    cacheSet.add(unionKey)
                    tuple = (1, 1)
                  }else{
                    cacheSet.add(unionKey)
                    tuple = (1, 0)
                  }
                }
                csts.+=((baseKey, tuple))
              })
            }
          }
        }finally {
          if (redisCache != null) redisCache.close()
        }
        csts.iterator
        //规约增量
      }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapPartitions(par => {
      //获取全量
      val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      try {
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "pv")
                val uvKey = getMarkKey(dateStr, baseRedisKey, record._1.key, "uv")
                arrayBuffer.+=((record._1, (resource.incrBy(pvKey, record._2._1), resource.incrBy(uvKey, record._2._2))))
                //设置失效时间
                resource.expireAt(pvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                resource.expireAt(uvKey, ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            )
        }
      } finally {
        if (redisCache != null) redisCache.close()
      }
      //全量RDD返回
      arrayBuffer.iterator
    })
  }

  /**
    * 获取 指定 key 下 pv uv 数据
    *
    * @param keys         DimensionKey keys
    * @param dateStr      date
    * @param baseRedisKey redis key prefix
    * @param redisPrefix  模块redis prefix
    * @tparam P
    * @return
    */
  def getCachedKeysPvUv[P <: DimensionKey : ClassTag : Ordering](keys: RDD[P], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): RDD[(P, (Long, Long))] = {
    keys.mapPartitions(par => {
      val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (Long, Long))]()
      try{
        use(redisCache.getResource) {
          resource =>
            par.foreach(
              record => {
                val pv = resource.get(getMarkKey(dateStr, baseRedisKey, record.key, "pv"))
                val uv = resource.get(getMarkKey(dateStr, baseRedisKey, record.key, "uv"))
                var pvVal = 0l
                var uvVal = 0l
                if (StringUtils.isNotBlank(pv)) {
                  pvVal = pv.toLong
                }
                if (StringUtils.isNotBlank(uv)) {
                  uvVal = uv.toLong
                }
                arrayBuffer.+=((record, (pvVal, uvVal))
                )
              }
            )
        }
      }finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }

  /**
    *
    * @param dateStr
    * @param baseKey
    * @param key
    * @param mark pv|uv
    * @return
    */
  def getMarkKey(dateStr: String, baseKey: String, key: String, mark: String): String = {
    s"$baseKey:$dateStr:$name:$key:$mark"
  }

  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:$name"
  }

}
