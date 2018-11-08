package com.ald.stat

import java.util.Date

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.utils.ComputeTimeUtils
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

object DelRedisKey extends App {
  val clientBaseKey = s"CLIENT:SIMPLE"
  val logger = LoggerFactory.getLogger(getClass)
  var count = 0
  var todayStr: String = null
  if (args != null && args.length != 0) todayStr = args(0)
  if (todayStr == null) {
    todayStr = ComputeTimeUtils.formatDate(DateUtils.addDays(new Date, -2), null)
    //如果没有就推算前两天
  }

  breakable {

    while (true) {

      lazy val redisCache = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache]
      val resources = redisCache.getResource.getAllShards.asScala
      resources.foreach(jedis => {
        try {
          deleteLastDateKey(jedis)
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }
        jedis.close()
        if (count == 0) break()
      })
      redisCache.getResource.close()
    }
  }



  def deleteLastDateKey(jedis: Jedis): Unit = {

    val pvKeys = s"*${todayStr}*"
    println(pvKeys)
    jedis.keys(pvKeys).asScala.foreach(key => {
      jedis.del(key)
      println(key)
    })
    println("  keys closed ")
  }


  logger.info("***************delete key completed {} *********", todayStr)

  //
  def deleteTopicKey(topic: String, redisCache: ClientRedisCache): Unit = {
    val bacthPrehandKey = topic + s":$todayStr:BATCH:PREHANDLE"
    val prehandKey = topic + s":$todayStr:PREHANDLE"
    //remove batch detail
    val sumIPKey = topic + s":IP:${todayStr}IP"
    val expandSimpleKey = topic + s":${todayStr}:SIMPLE:EXPAND"
    //﻿CLIENT:SIMPLE:COUNT:PVS:20180102
    val simpleCount = topic + s":COUNT:PVS:${todayStr}"
    val urlKey = s"$topic:$todayStr:URL"
    redisCache.del(urlKey)
    redisCache.del(simpleCount)
    //  ﻿//CLIENT:SIMPLE:20180102:SIMPLE:EXPAND
    redisCache.del(bacthPrehandKey)
    redisCache.del(prehandKey)
    redisCache.del(expandSimpleKey)
    val sumPVKey = topic + s":PVS:${todayStr}"
    val sumUidKey = topic + s":UID:${todayStr}UV"
    val resources = redisCache.getResource.getAllShards.asScala
    println("=======resource======" + resources.size)
    redisCache.del(sumIPKey)
    redisCache.del(sumPVKey)
    redisCache.del(sumUidKey)
    //        var todayStr = "20180102"

    resources.foreach(jedis => {
      val pvKeys = s"${topic}:${todayStr}*"
      println(pvKeys)
      jedis.keys(pvKeys).asScala.foreach(key => {
        //      println(key)
        jedis.del(key)
        println(key)
        //        count += 1
      })

    })
    resources.foreach(jedis => {
      val pvKeys = s"${topic}:SUM:${todayStr}*"
      println(pvKeys)
      jedis.keys(pvKeys).asScala.foreach(key => {
        //      println(key)
        jedis.del(key)
        println(key)
        //        count += 1
      })

    })
    resources.foreach(jedis => {
      val pvKeys = s"${topic}:IP:${todayStr}*"
      println(pvKeys)
      jedis.keys(pvKeys).asScala.foreach(key => {
        //      println(key)
        jedis.del(key)
        println(key)
        //        count += 1
      })

    })
    resources.foreach(jedis => {
      val pvKeys = s"${topic}:UID:${todayStr}*"
      println(pvKeys)
      jedis.keys(pvKeys).asScala.foreach(key => {
        //      println(key)
        jedis.del(key)
        println(key)
        //        count += 1
      })

    })
    resources.foreach(jedis => {
      val pvKeys = s"${topic}:Detail:${todayStr}*"
      println(pvKeys)
      jedis.keys(pvKeys).asScala.foreach(key => {
        //      println(key)
        jedis.del(key)
        println(key)
        //        count += 1
      })

    })
    resources.foreach(jedis => {
      val pvKeys = s"${topic}:COUNT:${todayStr}*"
      println(pvKeys)
      jedis.keys(pvKeys).asScala.foreach(key => {
        //      println(key)
        jedis.del(key)
        println(key)
        //        count += 1
      })

    })
  }

}
