package com.ald.stat.component.stat

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
import com.ald.stat.component.session.{SessionBase, SessionSum, SessionTrait}
import com.ald.stat.log.LogRecord
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

  def statSum[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag, P <: DimensionKey : ClassTag : Ordering]
  (rdd: RDD[(K, O)], statTrait: KeyParentTrait[C, K, P]): RDD[(P, SessionSum)] = {
    rdd.map((r) => {
      (statTrait.getBaseKey(r._1), generateSessionSum(r._2))
    }).reduceByKey((r1, r2) => {
      sumSessionSum(r1, r2)
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
        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
        par.foreach(record => {
          val redisKey = getSessionKey(dateStr, baseRedisKey)
          resource.expire(redisKey, Daily_Stat_Age)
          arrayBuffer.+=(handleSumSession(baseRedisKey, dateStr, resource, record, k))
        })
        if (resource != null) resource.close()
        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      latestSessionSum(s1, s2)
      //      sumSessionSum(s1,s2)
    })
    //      .map(
    //      record => {
    //        val sessionSumKey = record._1.key + ":sessionSum"
    //        val cacheSessionSumStr = resource.get(sessionSumKey)
    //        var finalSessionSumStr: String = ""
    //        var result = record;
    //        resource.expire(sessionSumKey, Daily_Stat_Age)
    //        if (StringUtils.isNotBlank(cacheSessionSumStr)) {
    //          val cacheSessionSum = toSumSession(cacheSessionSumStr.split(","))
    //          val finalSessionSum = sumSessionSum(record._2, cacheSessionSum)
    //          finalSessionSumStr = toSumSessionArray(finalSessionSum)
    //          result = (record._1, finalSessionSum)
    //        } else {
    //          finalSessionSumStr = toSumSessionArray(record._2)
    //        }
    //        resource.set(sessionSumKey, finalSessionSumStr)
    //        result
    //      }
    //    )

    //      .mapPartitions(par => {
    //        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
    //        val arrayBuffer = ArrayBuffer[(P, SessionSum)]()
    //        par.foreach(
    //          record => {
    //            val sessionSumKey = record._1.key + ":sessionSum"
    //            val cacheSessionSumStr = resource.get(sessionSumKey)
    //            var finalSessionSumStr: String = ""
    //            var result = record;
    //            resource.expire(sessionSumKey, Daily_Stat_Age)
    //            if (StringUtils.isNotBlank(cacheSessionSumStr)) {
    //              val cacheSessionSum = toSumSession(cacheSessionSumStr.split(","))
    //              val finalSessionSum = sumSessionSum(record._2, cacheSessionSum)
    //              finalSessionSumStr = toSumSessionArray(finalSessionSum)
    //              result = (record._1, finalSessionSum)
    //            } else {
    //              finalSessionSumStr = toSumSessionArray(record._2)
    //            }
    //            resource.set(sessionSumKey, finalSessionSumStr)
    //            arrayBuffer.+=(result)
    //          }
    //        )
    //        arrayBuffer.iterator
    //      })
  }

  def handleSumSession[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: SessionBase : ClassTag]
  (baseRedisKey: String, dateStr: String, resource: ShardedJedis, record: (K, O), k: KeyParentTrait[C, K, P]): (P, SessionSum) = {

    //这里搞一个二元组
    val redisKey = getSessionKey(dateStr, baseRedisKey)
    val baseKey = k.getBaseKey(record._1)
    val key = record._1.hashKey
    val cacheSessionJson = resource.hget(redisKey, key)
    //设置失效时间
    if (StringUtils.isNotBlank(cacheSessionJson)) {
      val sessionSum = new SessionSum
      try {
        val cacheSession = toSessionBase(cacheSessionJson.split(Sum_Delimiter))

        //处理新老用户，0是新用户
        if (record._2.newUser == 1 && cacheSession.newUser == 1) {
          sessionSum.newUserCount = 0l //说明不需要增加
        } else {
          record._2.newUser = 0
          if (cacheSession.newUser == 0)
            sessionSum.newUserCount = 0 //说明算过了
          else
            sessionSum.newUserCount = 1 //不同，说明没算过+1
        }

        //处理访问页数
        //record._2.url = URLUtils.getUrlNoParam(record._2.url)
        //record._2.firstUrl = record._2.url  //防止第二次没有设置
        //这个地方比较复杂，需要考虑到只有一个url的时候和一个session两个url的情况，
        //有一种情况是最后归并的url正好是不同的，所以合并url的时候，应该是做分解

        val urls1 = cacheSession.url.split(Url_Delimiter)
        val urls2 = record._2.url.split(Url_Delimiter)
        val newUrls = (urls1 ++ urls2).distinct
        record._2.url = newUrls.mkString(Url_Delimiter)
        record._2.pages = newUrls.length
        sessionSum.pagesSum = newUrls.length - cacheSession.pages
        if (newUrls.length > 1) {
          if (cacheSession.pages == 1) {
            //说明当时算过一次，应该减去1
            sessionSum.onePageofSession = -1l //这里应该都是算过了的
          }
        }
        //处理访问时长
        if (cacheSession.startDate < record._2.startDate) {
          record._2.firstUrl = cacheSession.firstUrl
          record._2.startDate = cacheSession.startDate
        }
        //把最大时间长度dr赋值sessionDurationSum
        if (cacheSession.sessionDurationSum < record._2.sessionDurationSum) {
          sessionSum.sessionDurationSum = record._2.sessionDurationSum
        }
        //补充到session里面计算
        //说明这个是已经算过的，因此主要是把更新的内容算出来
        //1.新用户，如果已经有了，计算的数量应该是0
        //2.处理访问页数，如果不同累计
        //3.如果累计数大于1，Singlepage原来是1，则要减1，否则现在是1，原来是1，则为0，如果是0则为1
        //4.如果时间增加的时间就是appendDuration
        resource.hset(redisKey, key, toSessionBaseArray(record._2))
        val finalSessionSum = increIndexSum(baseKey, sessionSum, resource)
        (baseKey, finalSessionSum)
      }
      catch {
        case t: Throwable => {
          logger.error(s"convert exeception $cacheSessionJson", t)
          record._2.pages = 1
          record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)
          val sessionSum = increIndexSum(baseKey, generateSessionSum(record._2), resource)
          (baseKey, sessionSum)
        }
      }
    }
    else {
      record._2.pageDurationSum = Math.abs(record._2.endDate - record._2.startDate)
      //这里
      resource.hset(redisKey, key, toSessionBaseArray(record._2))
      val sessionSum = increIndexSum(baseKey, generateSessionSum(record._2), resource)
      (baseKey, sessionSum)
    }
  }

  /**
    *
    * sessionsum
    *
    * @param sessionSum1
    * @param sessionSum2
    * @return
    */
  def latestSessionSum(sessionSum1: SessionSum, sessionSum2: SessionSum): SessionSum = {
    if (sessionSum1.updateTime > sessionSum2.updateTime) {
      return sessionSum1
    }
    return sessionSum2
  }

  /**
    *
    * @param k
    * @param sessionSum
    * @param resource
    * @tparam C
    * @tparam K
    * @return
    */
  def increIndexSum[C <: LogRecord, K <: DimensionKey](k: K, sessionSum: SessionSum, resource: ShardedJedis): SessionSum = {

    //key
    val newUserCountKey = k.key + ":newUser"
    val onePageCountKey = k.key + ":onePage"
    val sessionDurationKey = k.key + ":sessionDuration"
    val sessionCountKey = k.key + ":sessionCount"

    val newUserCount = resource.incrBy(newUserCountKey, sessionSum.newUserCount)
    val onePageCount = resource.incrBy(onePageCountKey, sessionSum.onePageofSession)
    val sessionDuration = resource.incrBy(sessionDurationKey, sessionSum.sessionDurationSum)
    val sessionCount = resource.incrBy(sessionCountKey, sessionSum.sessionCount)

    val sum = new SessionSum()
    sum.newUserCount = newUserCount
    sum.onePageofSession = onePageCount
    sum.sessionDurationSum = sessionDuration
    sum.sessionCount = sessionCount
    sum.updateTime = System.currentTimeMillis()
    sum
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
    o
  }

  def toSessionBaseArray(o: SessionBase): String = {
    Array(o.newUser, o.pages, o.url, o.startDate, o.endDate, o.firstUrl, o.ev).mkString(Sum_Delimiter)
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
    cs.updateTime = System.currentTimeMillis()
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
    o.updateTime = v(8).toLong
    o
  }

  def toSumSessionArray(o: SessionSum): String = {
    Array(o.pvs, o.uvs, o.ips, o.pagesSum, o.onePageofSession, o.sessionDurationSum, o.sessionCount, o.newUserCount, o.updateTime).mkString(",")
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


  def sumSessionSum[K <: SessionSum](r1: K, r2: K): SessionSum = {
    val sumSession = new SessionSum
    sumSession.pvs = r1.pvs + r2.pvs
    sumSession.uvs = r1.uvs + r2.uvs
    sumSession.ips = r1.ips + r2.ips
    sumSession.onePageofSession = r1.onePageofSession + r2.onePageofSession
    sumSession.pagesSum = r1.pagesSum + r2.pagesSum
    sumSession.newUserCount = r1.newUserCount + r2.newUserCount
    sumSession.sessionDurationSum = r1.sessionDurationSum + r2.sessionDurationSum
    sumSession.sessionCount = r1.sessionCount + r2.sessionCount
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

}
