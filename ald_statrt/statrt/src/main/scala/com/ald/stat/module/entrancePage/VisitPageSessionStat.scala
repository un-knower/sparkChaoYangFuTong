package com.ald.stat.module.entrancePage

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.link.link.DailyEntrancePageDimensionKey
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import redis.clients.jedis.ShardedJedis

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
  *
  * 入口页面计算
  */
trait VisitPageSessionStat extends EntrancePageStatBase {

  val logger = LoggerFactory.getLogger(this.getClass)
  val name = "entrancePage"

  def stat[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: EntrancePageSessionBase : ClassTag]
  (rdd: RDD[C], key: KeyTrait[C, K], session: EntrancePageSessionTrait[C, O]): RDD[(K, O)] = {
    rdd.map(record => (key.getKey(record), session.getEntity(record))).
      reduceByKey((r1, r2) => {
        mergeSession(r1, r2)
      })
  }

  def statSum[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, O <: EntrancePageSessionBase : ClassTag, P <: DimensionKey : ClassTag : Ordering]
  (rdd: RDD[(K, O)], statTrait: KeyParentTrait[C, K, P]): RDD[(P, EntrancePageSessionSum)] = {
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
  def doCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering, O <: EntrancePageSessionBase : ClassTag, R <: SessionSum : ClassTag]
  (baseRedisKey: String, taskId: String, dateStr: String, rdd: RDD[(K, O)], k: KeyParentTrait[C, K, P], redisPrefix: Broadcast[String]): RDD[(P, EntrancePageSessionSum)] = {
    rdd.mapPartitions(
      par => {
        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
        val arrayBuffer = ArrayBuffer[(P, EntrancePageSessionSum)]()
        par.foreach(record => {
          val x = (handleSumSession(baseRedisKey, dateStr, resource, record, k))
          arrayBuffer.+=((x._1, x._2))
          if (x._1.key.hashCode != x._3.key.hashCode && x._4 != null) {
            arrayBuffer.+=((x._3, x._4))
          }
        })
        if (resource != null) resource.close()
        arrayBuffer.iterator
      }).reduceByKey((s1, s2) => {
      sumSessionSum(s1, s2)
    })
  }

  def handleSumSession[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey : ClassTag : Ordering, O <: EntrancePageSessionBase : ClassTag]
  (baseRedisKey: String, dateStr: String, resource: ShardedJedis, record: (K, O), k: KeyParentTrait[C, K, P]): (P, EntrancePageSessionSum, P, EntrancePageSessionSum) = {

    //这里搞一个二元组
    val redisKey = getSessionKey(dateStr, baseRedisKey)
    val baseKey = k.getBaseKey(record._1)
    var oldBaseKey: P = k.getBaseKey(record._1)
    val key = record._1.hashKey
    //设置失效时间
    resource.expire(redisKey, Daily_Stat_Age)
    //session 入口页面hash key
    if (resource.hexists(redisKey, key)) {
      try {
        //补充到session里面计算
        //说明这个是已经算过的，因此主要是把更新的内容算出来
        //1.新用户，如果已经有了，计算的数量应该是0
        //2.处理访问页数，如果不同累计
        //3.如果累计数大于1，Singlepage原来是1，则要减1，否则现在是1，原来是1，则为0，如果是0则为1
        //4.如果时间增加的时间就是appendDuration
        val cacheSessionBaseStr = resource.hget(redisKey, key)
        val cacheSessionBase = toSessionBase(cacheSessionBaseStr.split(Sum_Delimiter))
        val mergedSessionBase = mergeSession(cacheSessionBase, record._2)

        val newEntrancePageSessionSum = new EntrancePageSessionSum
        val oldEntrancePageSessionSum = new EntrancePageSessionSum
        val currentPage = getCurrentPageOfKey(baseKey)
        newEntrancePageSessionSum.pv += 1
        newEntrancePageSessionSum.entrancePage = currentPage
        newEntrancePageSessionSum.sessionDurationSum += (mergedSessionBase.sessionDurationSum - cacheSessionBase.sessionDurationSum)

        if (cacheSessionBase.pages == 1 && mergedSessionBase.pages > 1) {
          newEntrancePageSessionSum.onePageOfSessionSum = -1
        }
        //即将规约的url和当前的入口页面相同
        if (currentPage == mergedSessionBase.firstUrl) {
          //判断入口页面是否变化
          if (record._2.firstUrl.trim == cacheSessionBase.firstUrl) {
            newEntrancePageSessionSum.firstPageOfSessionSum = 1
          } else {
            val lr = baseKey.lr.clone()
            lr.pp = cacheSessionBase.firstUrl
            //TODO:这里存在硬编码问题 DailyEntrancePageDimensionKey
            oldBaseKey = new DailyEntrancePageDimensionKey(lr).asInstanceOf[P]
            oldEntrancePageSessionSum.entrancePage = cacheSessionBase.firstUrl
            oldEntrancePageSessionSum.firstPageOfSessionSum = newEntrancePageSessionSum.firstPageOfSessionSum * (-1)
          }
        }
        resource.hset(redisKey, key, toSessionBaseArray(mergedSessionBase))
        (baseKey, newEntrancePageSessionSum, oldBaseKey, oldEntrancePageSessionSum)
      }
      catch {
        case t: Throwable => {
          logger.error(s"convert exeception ", t)
          record._2.pages = 1
          (baseKey, generateSessionSum(record._2), oldBaseKey, null)
        }
      }
    }
    else {
      resource.hset(redisKey, key, toSessionBaseArray(record._2))
      (baseKey, generateSessionSum(record._2), oldBaseKey, null)
    }
  }

  def toSessionBase(v: Array[String]): EntrancePageSessionBase = {
    val o = new EntrancePageSessionBase
    o.pageCount = v(0).toLong
    o.pages = v(1).toInt
    o.sessionDurationSum = v(2).toLong
    o.startDate = v(3).toLong
    o.url = v(4)
    o.firstUrl = v(5)
    o
  }

  def toSessionBaseArray(o: EntrancePageSessionBase): String = {
    Array(o.pageCount, o.pages, o.sessionDurationSum, o.startDate, o.url, o.firstUrl).mkString(Sum_Delimiter)
  }

  def generateSessionSum(entrancePageSessionBase: EntrancePageSessionBase): EntrancePageSessionSum = {
    //将数据转成下一步要计算的内容
    val cs = new EntrancePageSessionSum
    if (entrancePageSessionBase.pages == 1)
      cs.onePageOfSessionSum = 1
    else
      cs.onePageOfSessionSum = 0

    cs.pv = entrancePageSessionBase.pageCount
    cs.sessionDurationSum = entrancePageSessionBase.sessionDurationSum
    cs
  }

  def toSumSession(v: Array[String]): EntrancePageSessionSum = {
    val o = new EntrancePageSessionSum
    o.pv = v(0).toLong
    o.sessionDurationSum = v(5).toLong
    o
  }

  def toSumSessionArray(o: EntrancePageSessionSum): String = {
    Array(o.entrancePage, o.pv, o.onePageOfSessionSum, o.firstPageOfSessionSum, o.sessionDurationSum).mkString(",")
  }

  def sumSessionSum[K <: EntrancePageSessionSum](r1: K, r2: K): EntrancePageSessionSum = {
    val sumSession = new EntrancePageSessionSum
    sumSession.pv = r1.pv + r2.pv
    sumSession.onePageOfSessionSum = r1.onePageOfSessionSum + r2.onePageOfSessionSum
    sumSession.sessionDurationSum = r1.sessionDurationSum + r2.sessionDurationSum
    sumSession.firstPageOfSessionSum = r1.firstPageOfSessionSum + r2.firstPageOfSessionSum
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

  /**
    * 构建入口页
    *
    * @return
    */
  def getCurrentPageOfKey(dimensionKey: DimensionKey): String = {
    val splits = dimensionKey.key.split(":")
    if (splits.length == 3) {
      splits(2).trim
    } else {
      ""
    }
  }

  def getNewBaseKey(dimensionKey: DimensionKey, currentPage: String): String = {
    val splits = dimensionKey.key.split(":")
    if (splits.length == 3) {
      splits(0) + ":" + splits(1) + ":" + currentPage
    } else {
      ""
    }
  }
}
