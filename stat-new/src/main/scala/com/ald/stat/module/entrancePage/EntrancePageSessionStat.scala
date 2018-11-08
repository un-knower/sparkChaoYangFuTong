package com.ald.stat.module.entrancePage

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.link.link.DailyEntrancePageDimensionKey
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait, SubDimensionKey}
import com.ald.stat.component.session.SessionSum
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils
import org.apache.commons.lang3.StringUtils
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
trait EntrancePageSessionStat extends EntrancePageStatBase {

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
          val x = handleSumSession(baseRedisKey, dateStr, resource, record, k)
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
    val key = record._1.hashKey
    //设置失效时间
    resource.expireAt(redisKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
    //session 入口页面hash key

    val cacheSessionJson = resource.hget(redisKey, key)
    if (StringUtils.isNotBlank(cacheSessionJson)) {
      //返回的时候的新key
      val lr = record._1.lr
      var baseKey: P = new DailyEntrancePageDimensionKey(lr).asInstanceOf[P]
      //返回的时候的old key
      var oldBaseKey: P = new DailyEntrancePageDimensionKey(lr).asInstanceOf[P]

      try {
        //1.补充到session里面计算
        //2.处理访问页数，如果不同累计
        //3.如果累计数大于1，Singlepage原来是1，则要减1，否则现在是1，原来是1，则为0，如果是0则为1
        //4.如果时间增加的时间就是appendDuration
        //缓存中的session相关入口页信息
        val cacheSessionBase = toSessionBase(cacheSessionJson.split(Sum_Delimiter))
        val oldSessionSum = new EntrancePageSessionSum
        var newSessionSum = new EntrancePageSessionSum
        var isChange = false

        if (cacheSessionBase.startDate <= record._2.startDate) {
          record._2.startDate = cacheSessionBase.startDate
          record._2.firstUrl = cacheSessionBase.firstUrl
          record._2.sessionDurationSum = cacheSessionBase.sessionDurationSum
          record._2.onePageOfSessionSum = cacheSessionBase.onePageOfSessionSum
          record._2.firstPageOfSessionSum = cacheSessionBase.firstPageOfSessionSum
        } else {
          //入口页面变化
          if (!cacheSessionBase.firstUrl.equals(record._2.firstUrl)) {
            isChange = true
          } else {
            record._2.onePageOfSessionSum = cacheSessionBase.onePageOfSessionSum
            record._2.firstPageOfSessionSum = cacheSessionBase.firstPageOfSessionSum
            //非变化情况下
            if (cacheSessionBase.sessionDurationSum < record._2.sessionDurationSum) {
              newSessionSum.sessionDurationSum = record._2.sessionDurationSum - cacheSessionBase.sessionDurationSum
            }
          }
        }
        //处理访问页数
        //record._2.url = URLUtils.getUrlNoParam(record._2.url)
        //record._2.firstUrl = record._2.url  //防止第二次没有设置
        //这个地方比较复杂，需要考虑到只有一个url的时候和一个session两个url的情况，
        //有一种情况是最后归并的url正好是不同的，所以合并url的时候，应该是做分解
        val urls1 = cacheSessionBase.url.split(Url_Delimiter)
        val urls2 = record._2.url.split(Url_Delimiter)
        val newUrls = (urls1 ++ urls2).distinct
        record._2.url = newUrls.mkString(Url_Delimiter)
        record._2.pages = newUrls.length
        newSessionSum.pages = newUrls.length - cacheSessionBase.pages
        if (newUrls.length > 1) {
          if (cacheSessionBase.pages == 1) {
            //说明当时算过一次，应该减去1
            oldSessionSum.onePageOfSessionSum = -1l
          }
        }
        //不管入口页面是否改变，则新建baseKey
        lr.pp = record._2.firstUrl
        baseKey = new DailyEntrancePageDimensionKey(lr).asInstanceOf[P]

        if (isChange) {
          //如果改变入口页，则新建oldBaseKey
          lr.pp = cacheSessionBase.firstUrl
          oldBaseKey = new DailyEntrancePageDimensionKey(lr).asInstanceOf[P]
          oldSessionSum.firstPageOfSessionSum = -cacheSessionBase.firstPageOfSessionSum
          oldSessionSum.sessionDurationSum = -cacheSessionBase.sessionDurationSum
          oldSessionSum.uv = -cacheSessionBase.uv
          oldSessionSum.pv = -cacheSessionBase.pageCount
          oldSessionSum.openCount = -cacheSessionBase.openCount
          //构造新增量
          newSessionSum = generateSessionSum(record._2)
        }
        resource.hset(redisKey, key, toSessionBaseArray(record._2))
        (baseKey, newSessionSum, oldBaseKey, oldSessionSum)
      }
      catch {
        case t: Throwable => {
          logger.error(s"convert exeception ", t)
          record._2.pages = 1
          (baseKey, generateSessionSum(record._2), oldBaseKey, null)
        }
      }
    } else {
      resource.hset(redisKey, key, toSessionBaseArray(record._2))
      val baseKey = new DailyEntrancePageDimensionKey(record._1.lr).asInstanceOf[P]
      (baseKey, generateSessionSum(record._2), baseKey, null)
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
    cs.firstPageOfSessionSum = 1
    cs.pv = entrancePageSessionBase.pageCount
    cs.uv = entrancePageSessionBase.uv
    cs.openCount = entrancePageSessionBase.openCount
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
    sumSession.uv = r1.uv +r2.uv
    sumSession.openCount = r1.openCount + r2.openCount
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
