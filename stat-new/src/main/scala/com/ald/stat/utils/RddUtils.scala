package com.ald.stat.utils

import java.io.Serializable

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.session.SessionSum
import com.ald.stat.module.session.SessionStatImpl
import com.ald.stat.utils.DBUtils.use
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.ShardedJedis

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object RddUtils {

  val Daily_Stat_Age = 1 * 25 * 3600


  /**
    *
    * 保存offset
    *
    * @param dateStr
    * @param topic
    * @param group
    * @param line
    * @param resource
    */
  //TODO:这里可能存在效率问题
  def checkAndSaveOffset(dateStr: String, topic: String, group: String, line: ConsumerRecord[String, String], resource: ShardedJedis) = {

    //每个分区进行和redis进行比较
    val minOffsetKey = RedisUtils.buildOffsetKey(dateStr, topic, group, line.partition(), "min")
    val latestOffsetKey = RedisUtils.buildOffsetKey(dateStr, topic, group, line.partition(), "latest")

    //最小offset 处理
    val minRedisOffset = resource.get(minOffsetKey)
    if (StringUtils.isNotBlank(minRedisOffset)) {
      //swap
      if (line.offset() < minRedisOffset.toLong) {
        resource.set(minOffsetKey, line.offset().toString)
      }
    } else {
      resource.set(minOffsetKey, line.offset().toString)
    }
    //最大offset处理
    val latestRedisOffset = resource.get(latestOffsetKey)
    if (StringUtils.isNotBlank(latestRedisOffset)) {
      //swap
      if (line.offset() > latestRedisOffset.toLong) {
        resource.set(latestOffsetKey, (line.offset() + 1).toString)
      }
    } else {
      resource.set(latestOffsetKey, (line.offset() + 1).toString)
    }

    resource.expireAt(minOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
    resource.expireAt(latestOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
  }

  /**
    * 检查并维护最小的offset
    * @param dateStr
    * @param topic
    * @param group
    * @param line
    * @param resource
    * @return
    */
  def checkAndSaveMinOffset(dateStr: String, topic: String, group: String, line: ConsumerRecord[String, String], resource: ShardedJedis) = {
    val minOffsetKey = RedisUtils.buildOffsetKey(dateStr, topic, group, line.partition(), "min") //当天最小的offset
    //维护当天最小的offset
    val minRedisOffset = resource.get(minOffsetKey)
    if (StringUtils.isNotBlank(minRedisOffset)) {
      if (line.offset() < minRedisOffset.toLong) {
        resource.set(minOffsetKey, line.offset().toString)
      }
    } else {
      resource.set(minOffsetKey, line.offset().toString)
    }
    resource.expireAt(minOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
  }

  /**
    * 检查并维护最大的offset
    * @param dateStr
    * @param topic
    * @param group
    * @param line
    * @param resource
    * @return
    */
  def checkAndSaveLatestOffset(dateStr: String, topic: String, group: String, line: ConsumerRecord[String, String], resource: ShardedJedis) = {
    //每个分区进行和redis进行比较
    val latestOffsetKey = RedisUtils.buildOffsetKey(dateStr, topic, group, line.partition(), "latest")

    //最大offset处理
    val latestRedisOffset = resource.get(latestOffsetKey)
    if (StringUtils.isNotBlank(latestRedisOffset)) {
      //swap
      if (line.offset() > latestRedisOffset.toLong) {
        resource.set(latestOffsetKey, (line.offset() + 1).toString)
      }
    } else {
      resource.set(latestOffsetKey, (line.offset() + 1).toString)
    }

    resource.expireAt(latestOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
  }


  /**
    *
    * 保存offset
    *
    * @param dateStr
    * @param topic
    * @param group
    * @param resource
    */
  def checkAndSaveOffset(dateStr: String, topic: String, group: String, offsetRanges: Array[OffsetRange], resource: ShardedJedis) = {

    offsetRanges.foreach(
      o => {
        //每个分区进行和redis进行比较
        val minOffsetKey = RedisUtils.buildOffsetKey(dateStr, topic, group, o.partition, "min")
        val latestOffsetKey = RedisUtils.buildOffsetKey(dateStr, topic, group, o.partition, "latest")
        val currentOffset = o.fromOffset + 1
        //最小offset 处理
        val minRedisOffset = resource.get(minOffsetKey)
        if (StringUtils.isNotBlank(minRedisOffset)) {
          //swap
          if (currentOffset < minRedisOffset.toLong) {
            resource.set(minOffsetKey, currentOffset.toString)
          }

        } else {
          resource.set(minOffsetKey, currentOffset.toString)
        }
        //最大offset处理
        val latestRedisOffset = resource.get(latestOffsetKey)
        if (StringUtils.isNotBlank(latestRedisOffset)) {
          //swap
          if (currentOffset > latestRedisOffset.toLong) {
            resource.set(latestOffsetKey, currentOffset.toString)
          }
        } else {
          resource.set(latestOffsetKey, currentOffset.toString)
        }

        resource.expireAt(minOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
        resource.expireAt(latestOffsetKey,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
      }
    )
  }

  def saveRDD[K <: Serializable](rdd: RDD[K], url: String): Unit = {
    rdd.saveAsObjectFile(url)
  }

  /**
    * 处理分批数据 key 不一致
    *
    * @param baseRedisKey
    * @param dateStr
    * @param share_pv_uv_rdd
    * @param back_uv_op_rdd
    * @param sessionRDD
    * @param redisPrefix
    * @tparam P
    * @return
    */
  def shareRddUnionHandle[P <: DimensionKey : ClassTag : Ordering, K <: SessionSum](baseRedisKey: String, dateStr: String, shareRedisMark: String, backShareRedisMark: String, sessionRedisMark: String, share_pv_uv_rdd: RDD[(P, (Long, Long))], back_uv_op_rdd: RDD[(P, (Long, Long))], sessionRDD: RDD[(P, SessionSum)], redisPrefix: Broadcast[String]): RDD[(DimensionKey, ((Long, Long), ((Long, Long), SessionSum)))] = {

    share_pv_uv_rdd.fullOuterJoin((back_uv_op_rdd).fullOuterJoin(sessionRDD)).
      map(v => {
        val defaultTriple = (Option((0l, 0l)), Option(new SessionSum))
        val share_pv_uv = v._2._1.getOrElse(0l, 0l)
        val back_pv_uv = v._2._2.getOrElse(defaultTriple)._1.get
        val sessionStat = v._2._2.getOrElse(defaultTriple)._2.get
        (v._1, (share_pv_uv, (back_pv_uv, sessionStat)))
      })
      .reduceByKey((a, b) => {
        val share_pv_uv = (a._1._1 + b._1._1, a._1._2 + b._1._2)
        val back_pv_uv = (a._2._1._1 + b._2._1._1, a._2._1._2 + b._2._1._2)
        val sessionStat = SessionStatImpl.sumSessionSum(a._2._2, b._2._2, true)
        (share_pv_uv, (back_pv_uv, sessionStat))
      })
      .mapPartitions(
        par => {
          val redisCache = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache]
          val arrayBuffer = ArrayBuffer[(DimensionKey, ((Long, Long), ((Long, Long), SessionSum)))]()
          use(redisCache.getResource) {
            resource =>
              par.foreach(
                record => {
                  //sessionSum
                  //share
                  val share_pv_uv = record._2._1
                  var share_pv_val = share_pv_uv._1
                  var share_uv_val = share_pv_uv._2
                  //share pv
                  val sharePvKey = RedisUtils.buildPvUvMarkKey(dateStr, baseRedisKey, record._1.key, shareRedisMark, "pv")
                  if (share_pv_uv._1 == 0) {
                    val pv = resource.get(sharePvKey)
                    if (StringUtils.isNotBlank(pv)) {
                      share_pv_val = pv.toLong
                    }
                  }
                  //share uv
                  val shareUvKey = RedisUtils.buildPvUvMarkKey(dateStr, baseRedisKey, record._1.key, shareRedisMark, "uv")
                  if (share_pv_uv._2 == 0) {
                    val uv = resource.get(shareUvKey)
                    if (StringUtils.isNotBlank(uv)) {
                      share_uv_val = uv.toLong
                    }
                  }
                  val share_pv_uv_ret = (share_pv_val, share_uv_val)
                  //back
                  val back_pv_uv = record._2._1
                  var back_pv_val = back_pv_uv._1
                  var back_uv_val = back_pv_uv._2

                  val backSharePvKey = RedisUtils.buildPvUvMarkKey(dateStr, baseRedisKey, record._1.key, backShareRedisMark, "pv")
                  //share pv
                  if (back_pv_uv._1 == 0) {
                    val pv = resource.get(backSharePvKey)
                    if (StringUtils.isNotBlank(pv)) {
                      back_pv_val = pv.toLong
                    }
                  }
                  val backShareUvKey = RedisUtils.buildPvUvMarkKey(dateStr, baseRedisKey, record._1.key, backShareRedisMark, "uv")
                  //back uv
                  if (back_pv_uv._2 == 0) {
                    val uv = resource.get(backShareUvKey)
                    if (StringUtils.isNotBlank(uv)) {
                      back_uv_val = uv.toLong
                    }
                  }
                  val back_pv_uv_ret = (back_pv_val, back_uv_val)
                  //sessionSum
                  var sessionSum = record._2._2._2
                  val cacheSessionKey = RedisUtils.buildSessionSumKey(dateStr, baseRedisKey, sessionRedisMark)
                  val sessionKey = record._1.hashKey
                  val cacheSessionSumStr = resource.hget(cacheSessionKey, sessionKey)
                  if (StringUtils.isNotBlank(cacheSessionSumStr)) {
                    sessionSum = SessionStatImpl.toSumSession(cacheSessionSumStr.split(","))
                  }
                  arrayBuffer.+=((record._1, ((share_pv_uv_ret), ((back_pv_uv_ret), sessionSum))))
                }
              )
          }
          arrayBuffer.iterator
        }
      )
  }


}

