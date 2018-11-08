package com.ald.stat.raw

import com.ald.stat.cache.{CacheClientFactory, CacheFactory, ClientRedisCache, RedisCache}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
  * 用于处理原始log的工具类
  */

object RawOperation {

  lazy val redisClient: RedisCache = CacheFactory.getInstances
  lazy val clientRedisClient: ClientRedisCache = CacheClientFactory.getInstances.asInstanceOf[ClientRedisCache]
  val logger = LoggerFactory.getLogger("[RawOperation]")



  def empty2unknown(v: String): String = {
    if (StringUtils.isEmpty(v)) {
      "unknown"
    }
    else {
      v
    }
  }


//  /**
//    * 时间校正
//    * 昨天以前和超过今天的数据丢弃
//    *
//    * @param logRecord
//    * @return
//    */
//  def timeCorrect(logRecord: DetailLogRecord, todayStr: String): DetailLogRecord = {
//    val timeD = timesDiffence((logRecord.st).toLong, logRecord.st.toLong)
//    logRecord.st=(logRecord.st.toLong + timeD * 1000).toString //时间校正
//    if (ComputeTimeUtils.inDay(todayStr, logRecord.st.toLong)) {
//      logRecord
//    }
//    else
//      null
//  }




}
