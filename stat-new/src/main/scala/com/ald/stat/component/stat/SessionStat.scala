package com.ald.stat.component.stat

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
import com.ald.stat.component.session.{SessionBase, SessionSum, SessionTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}
import com.ald.stat.utils.DBUtils.use
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import redis.clients.jedis.ShardedJedis

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait SessionStat extends StatBase {

  val logger = LoggerFactory.getLogger(this.getClass)
  val name = "SESSION"

  def stat[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (rdd: RDD[C], key: KeyTrait[C, K], session: SessionTrait[C, O]): RDD[(K, O)] = {
    rdd.map(record => (key.getKey(record), session.getEntity(record)))
      .reduceByKey((r1, r2) => {
        mergeSession(r1, r2)
      })
  }

  /**
    * 补偿计算session
    * @param baseRedisKey
    * @param taskId
    * @param dateStr
    * @param rdd
    * @param k
    * @param redisPrefix
    * @tparam C
    * @tparam K
    * @tparam P
    * @tparam O
    * @tparam R
    * @return
    */
  def statPathSession[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, taskId: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, SessionSum)] = {
    rdd.mapPartitions(
      par => {
        val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handlePatchSumSession(baseRedisKey, dateStr, resource, record, k))
              })
          }
        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {
      val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
      val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
      use(redisCache.getResource) {
        resource =>
          val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
          par.foreach(
            record => {
              val sessionKey = record._1.hashKey
              val final_key = getFinalKey(cacheSessionKey,sessionKey)//获取最终的key
              resource.set(final_key, toSumSessionArray(record._2)) //直接用计算结果覆盖缓存中的结果
              resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              arrayBuffer.+=((record))
            }
          )
      }
      arrayBuffer.iterator
    })
  }

  /**
    * 获取补偿时的sumSession
    * @param baseRedisKey
    * @param dateStr
    * @param resource
    * @param record
    * @param k
    * @tparam C
    * @tparam K
    * @tparam P
    * @tparam O
    * @return
    */
  def handlePatchSumSession[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (baseRedisKey: String, dateStr: String, resource: ShardedJedis, record: (K, O), k: KeyParentTrait[C, K, P]): (P, SessionSum) = {
    val redisKey = getSessionKey(dateStr, baseRedisKey)
    val baseKey = k.getBaseKey(record._1)
    val key = record._1.hashKey
    val final_key = getFinalKey(redisKey,key)//获取最终的key
    record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)
    if(resource.exists(final_key)==false){
      resource.set(final_key,toSessionBaseArray(record._2))
      resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
    }
    (baseKey, generateSessionSum(record._2))
  }

  def statSum[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, P <: DimensionKey : ClassTag : Ordering]
  (rdd: RDD[(K, O)], statTrait: KeyParentTrait[C, K, P]): RDD[(P, SessionSum)] = {
    rdd.map((r) => {
      (statTrait.getBaseKey(r._1), generateSessionSum(r._2))
    }).reduceByKey((r1, r2) => {
      sumSessionSum(r1, r2, true)
    })
  }


  /**
    *
    * @param baseRedisKey
    * @param taskId 批次ID
    * @param dateStr
    * @param rdd
    * @param k
    * @tparam C
    * @tparam K
    * @tparam P
    * @tparam O
    * @tparam R
    * @return
    */
  def doCacheWithRedisMark[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, taskId: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, (SessionSum, String))] = {

    rdd.mapPartitions(
      par => {
        val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        try{
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handleSumSession(baseRedisKey, dateStr, resource, record, k))
              })
              //设置失效时间
              val redisKey = getSessionKey(dateStr, baseRedisKey)
              resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          }
        }finally {
          if (redisCache != null) redisCache.close()
        }

        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {
      val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
      val arrayBuffer = ArrayBuffer[(P, (SessionSum, String))]()
      try{
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record._1.hashKey
                val cacheSessionSumStr = resource.hget(cacheSessionKey, sessionKey)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  val mergedSessionSum = sumSessionSum(sessionSum, record._2, true)
                  resource.hset(cacheSessionKey, sessionKey, toSumSessionArray(mergedSessionSum))
                  arrayBuffer.+=((record._1, (mergedSessionSum, name)))
                } else {
                  resource.hset(cacheSessionKey, sessionKey, toSumSessionArray(record._2))
                  arrayBuffer.+=((record._1, (record._2, name)))
                }
              }
            )
            resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      }finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }



  /**
    *
    * @param baseRedisKey
    * @param taskId 批次ID
    * @param dateStr
    * @param rdd
    * @param k
    * @tparam C
    * @tparam K
    * @tparam P
    * @tparam O
    * @tparam R
    * @return
    */
  def doCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, taskId: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, SessionSum)] = {

    rdd.mapPartitions(
      par => {
        val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        try{
          use(redisCache.getResource) {
            resource =>
              par.foreach(record => {
                arrayBuffer.+=(handleSumSession(baseRedisKey, dateStr, resource, record, k))
              })
              //设置失效时间
              //val redisKey = getSessionKey(dateStr, baseRedisKey)
              //resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
          }
        }finally {
          if (redisCache != null) redisCache.close()
        }

        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2, true)
    }).mapPartitions(par => {

      val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
      val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
      try{
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record._1.hashKey
                val final_key = getFinalKey(cacheSessionKey,sessionKey)//获取最终的key
                val cacheSessionSumStr = resource.get(final_key)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  val mergedSessionSum = sumSessionSum(sessionSum, record._2, true)
                  resource.set(final_key, toSumSessionArray(mergedSessionSum))
                  resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record._1, mergedSessionSum))
                } else {
                  resource.set(final_key, toSumSessionArray(record._2))
                  resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  arrayBuffer.+=((record))
                }
              }
            )
          //resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      }finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }

  def handleSumSession[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (baseRedisKey: String, dateStr: String, resource: ShardedJedis, record: (K, O), k: KeyParentTrait[C, K, P]): (P, SessionSum) = {

    //这里搞一个二元组
    val redisKey = getSessionKey(dateStr, baseRedisKey)
    val baseKey = k.getBaseKey(record._1)
    val key = record._1.hashKey
    val final_key = getFinalKey(redisKey,key)//获取最终的key
    val cacheSessionJson = resource.get(final_key)//平铺后，直接采用get方式获取
    if (StringUtils.isNotBlank(cacheSessionJson)) {
      try {
        val cacheSession = toSessionBase(cacheSessionJson.split(Sum_Delimiter))
        val oldSessionDuration = cacheSession.sessionDurationSum
        val mergedSessionBase = mergeSession(cacheSession, record._2) //当前批次与缓存中进行聚合

        resource.set(final_key, toSessionBaseArray(mergedSessionBase))
        resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期

        val sessionSum = generateSessionSum(mergedSessionBase)
        if(cacheSession.newUser  == 0)sessionSum.newUserCount = 0 //如果缓存中已经是新用户，则此时不能再进行新用户的计数了
        sessionSum.sessionCount = 0 //因为该会话已经在缓存中，存在，所以不能重复计数

        if(cacheSession.pages == 1){//如果缓存中pages=1，则说明该会话已经被记录过一次跳出数
          if(mergedSessionBase.pages > 1){//如果当前批次与缓存中聚合后，发现聚合后pages>1,则说明这个会话已经不是跳出的会话了
            sessionSum.onePageofSession = -1//此时应当减去之前记录进去的跳出数
          }else if (mergedSessionBase.pages == 1){//如果当前批次与缓存中聚合后，pages依然为1，则说明该会话重复访问了一个页面
            sessionSum.onePageofSession = 0//此时不再重复记录跳出数
          }
        }

        sessionSum.sessionDurationSum = mergedSessionBase.sessionDurationSum - oldSessionDuration
        (baseKey, sessionSum)
      }
      catch {
        case t: Throwable => {
          logger.error(s"convert exeception $cacheSessionJson", t)
          record._2.pages = 1
          record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)

          resource.set(final_key,toSessionBaseArray(record._2))
          resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期

          (baseKey, generateSessionSum(record._2))
        }
      }
    }
    else {
      record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)

      resource.set(final_key,toSessionBaseArray(record._2))
      resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期

      (baseKey, generateSessionSum(record._2))
    }
  }

  /**
    * 直接读取已经存储在redis key 的全量数据
    *
    * @param keys         DimensionKey
    * @param dateStr      20180101
    * @param baseRedisKey redis key prefix
    * @param redisPrefix  模块redis prefix
    * @tparam P 范型
    * @return
    */
  def getCachedKeysStat[P <: DimensionKey : ClassTag : Ordering](keys: RDD[P], dateStr: String, baseRedisKey: String, redisPrefix: Broadcast[String]): RDD[(P, SessionSum)] = {
    keys.mapPartitions(par => {
      val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
      val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
      try{
        use(redisCache.getResource) {
          resource =>
            val cacheSessionKey = getSessionSumKey(dateStr, baseRedisKey)
            par.foreach(
              record => {
                val sessionKey = record.hashKey
                val cacheSessionSumStr = resource.hget(cacheSessionKey, sessionKey)
                if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                  val sessionSum = toSumSession(cacheSessionSumStr.split(","))
                  arrayBuffer.+=((record, sessionSum))
                }
              }
            )
            resource.expireAt(cacheSessionKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        }
      }finally {
        if (redisCache != null) redisCache.close()
      }

      arrayBuffer.iterator
    })
  }


  def toSessionBase(v: Array[String]): SessionBase = {
    val o = new SessionBase
    o.newUser = v(0).toInt
    o.pages = v(1).toInt
    o.url = v(2)
    o.startDate = v(3).toLong
    o.endDate = v(4).toLong
    o.firstUrl = v(5)
    o.ev = v(6)
    try {
      o.sessionDurationSum = v(7).toLong
    } catch {
      case _: NumberFormatException => {
        o.sessionDurationSum = 1
      }
    }
    o
  }

  def toSessionBaseArray(o: SessionBase): String = {
    Array(o.newUser, o.pages, o.url, o.startDate, o.endDate, o.firstUrl, o.ev, o.sessionDurationSum).mkString(Sum_Delimiter)
  }

  def generateSessionSum(sessionBase: SessionBase): SessionSum = {
    val cs = new SessionSum //将数据转成下一步要计算的内容
    if (sessionBase.newUser == 0)
      cs.newUserCount = 1 //新用户
    if (sessionBase.pages == 1)
      cs.onePageofSession = 1
    else
      cs.onePageofSession = 0
    cs.sessionCount = 1
    cs.sessionDurationSum = sessionBase.sessionDurationSum
    cs.pagesSum = sessionBase.pages
    cs
  }

  def toSumSession(v: Array[String]): SessionSum = {
    val o = new SessionSum
    o.pvs = v(0).toLong
    o.uvs = v(1).toLong
    o.ips = v(2).toLong
    o.pagesSum = v(3).toLong
    o.onePageofSession = v(4).toLong
    o.sessionDurationSum = v(5).toLong
    o.sessionCount = v(6).toLong
    o.newUserCount = v(7).toLong
    o
  }

  def toSumSessionArray(o: SessionSum): String = {
    Array(o.pvs, o.uvs, o.ips, o.pagesSum, o.onePageofSession, o.sessionDurationSum, o.sessionCount, o.newUserCount).mkString(",")
  }


  def statSessionSum(r1: SessionSum, clientSession: SessionBase, oldDuration: Long, appendDuration: Long): SessionSum = {

    if (clientSession.pages == 1)
      r1.onePageofSession += 1
    r1.pagesSum += clientSession.pages
    //这个地方比较复杂一点，主要是要去掉补偿的问题
    if (appendDuration == 0) //无需补偿
      r1.sessionDurationSum += clientSession.pageDurationSum
    else {
      r1.sessionDurationSum += appendDuration - oldDuration
    }
    r1.sessionCount += 1
    if (clientSession.newUser == 0)
      r1.newUserCount += 1
    r1
  }


  def sumSessionSum[K <: SessionSum](r1: K, r2: K, doSessionCountSum: Boolean): SessionSum = {
    val sumSession = new SessionSum
    sumSession.pvs = r1.pvs + r2.pvs
    sumSession.uvs = r1.uvs + r2.uvs
    sumSession.ips = r1.ips + r2.ips
    sumSession.onePageofSession = r1.onePageofSession + r2.onePageofSession
    sumSession.pagesSum = r1.pagesSum + r2.pagesSum
    sumSession.newUserCount = r1.newUserCount + r2.newUserCount
    sumSession.sessionDurationSum = r1.sessionDurationSum + r2.sessionDurationSum
    if (doSessionCountSum) {
      sumSession.sessionCount = r1.sessionCount + r2.sessionCount
    } else {
      sumSession.sessionCount = 1
    }
    sumSession
  }


  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr$name"
  }

  def getSessionKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:Detail:$name"
  }

  def getSessionSumKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:SUM:$name"
  }

  def getFinalKey(redisKey:String,key:String): String = {
    val expand_Key = redisKey + ":" + key //展开后的key
    val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
    val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key

    final_key
  }

}
