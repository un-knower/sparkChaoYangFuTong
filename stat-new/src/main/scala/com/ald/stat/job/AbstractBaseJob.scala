package com.ald.stat.job

import java.util
import java.util.Date

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.job.AnalysisTrend.offsetPrefix
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils, RddUtils}
import com.ald.stat.utils.DBUtils.{getConnection, use}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.ShardedJedis

/**
  *
  */
class AbstractBaseJob {


  /**
    * 对old sdk 处理new user
    *
    * @param stream
    * @param baseRedisKey
    */
  def handleNewUserForOldSDK(stream: InputDStream[ConsumerRecord[String, String]], baseRedisKey: String, prefix: String): Unit = {
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
        try{
          use(redisCache.getResource) {
            resource =>
              par.foreach(line => {
                val logRecord = LogRecord.line2Bean(line.value())
                if (logRecord != null && logRecord.ifo == "true"
                  && logRecord.v < "7.0.0") {
                  val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
                  val key = s"$baseRedisKey:$d:newUser"
                  resource.hset(key, logRecord.at.trim, "true")
                  resource.expireAt(key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                }
              })
          }
        }finally {
          if (redisCache != null) redisCache.close()
        }
      })
    })
  }


  /**
    * 标记新用户并维护最大的offset
    * @param stream
    * @param group
    * @param baseRedisKey
    * @param prefix
    * @param offset_prefix
    */
  def handleNewUserAndLatestOffset(stream: InputDStream[ConsumerRecord[String, String]], group: String,
                                   baseRedisKey: String, prefix: String,offset_prefix: String): Unit = {
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        var latest_record:ConsumerRecord[String, String] = null
        val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
        try{
          use(redisCache.getResource) {
            resource =>
              par.foreach(line => {
                val logRecord = LogRecord.line2Bean(line.value())
                if (logRecord != null && logRecord.at != null &&
                  logRecord.et != null && logRecord.ifo == "true" && logRecord.v < "7.0.0") {
                  val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
                  val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim  //展开后的key
                  val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
                  val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key

                  if (resource.exists(final_key) == false){//如果不存在则添加
                    resource.set(final_key,"true")
                    resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
                  }
                }
                latest_record = line //遍历，赋值，最终得到最后一条记录
              })
          }
        }finally {
          if (redisCache != null) redisCache.close()
        }
        //维护最大的offset
        if(latest_record != null){
          val dateStr = ComputeTimeUtils.getDateStr(new Date())
          val offsetRedisCache = CacheClientFactory.getInstances(offset_prefix).asInstanceOf[ClientRedisCache]
//          val resource_offset = offsetRedisCache.getResource
          use(offsetRedisCache.getResource) { resource_offset =>
            RddUtils.checkAndSaveLatestOffset(dateStr, latest_record.topic(), group, latest_record, resource_offset)
          }
//          try{

//          }finally {
//            if (resource_offset != null) resource_offset.close()
//            if (offsetRedisCache != null) offsetRedisCache.close()
//          }
        }

      })
    })
  }


  /**
    * 对old sdk 处理new user
    * 因为我们的程序上线之后日志有7.0.0版本的，也有6.0版本的。所以这个函数是用来将7.0以下的版本的新用户数uu，进行处理
    *
    *
    * rdd
    *
    * @param baseRedisKey
    */
  def handleNewUserForOldSDK(rdd: RDD[ConsumerRecord[String, String]], baseRedisKey: String, prefix: String): Unit = {
    rdd.foreachPartition(par => {
      //gcs:获得一个redis的实例对象
      val redisCache = CacheClientFactory.getInstances(prefix).asInstanceOf[ClientRedisCache]
      use(redisCache.getResource) {
        resource =>
          par.foreach(line => {
            val logRecord = LogRecord.line2Bean(line.value()) //gcs:将我们的原始日志转换为logRecord对象

            //gcs:将新用户数添加到redis当中
            //&& logRecord.v < "7.0.0"
            if (logRecord != null && logRecord.at != null && logRecord.et != null && logRecord.ifo == "true" && logRecord.v < "7.0.0") {
              val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)

              //gcs:这里涉及到了一个redis的优化。因为在我们用uu+维度形成的key中，会存在数据倾斜的风险，后期也证明了，确实是redis是存在数据倾
              // 斜的，这导致了有的redis的分片满了，但是有的redis的分片却还很充足。因此，我们将我们的Key做了hash，即将expend_key先做
              // 了hash,之后将产生的hash值拼接到了expend_key的前面，形成了最终的final_key，这个final_key是加盐之后的Key
              val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim  //展开后的key
              val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
              val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key

              if (resource.exists(final_key) == false){//如果不存在则添加
                resource.set(final_key,"true")
                resource.expireAt(final_key,ComputeTimeUtils.getRepireKeyTime) //设置为次日凌晨2点过期
              }
            }
          })
      }
    })
  }

  /**
    * 标记 新用户
    *
    * @param logRecord
    * @param resource
    * @param baseRedisKey
    * @return
    */
  def markNewUser(logRecord: LogRecord, resource: ShardedJedis, baseRedisKey: String): LogRecord = {
    val d = ComputeTimeUtils.getDateStr(logRecord.et.toLong)
    val expand_Key = s"$baseRedisKey:$d:newUser" + ":" + logRecord.at.trim  //展开后的keyx
    val expand_Key_hash = HashUtils.getHash(expand_Key).toString  //对整个key取hash值
    val final_key = expand_Key_hash + ":" + expand_Key  //将完整的key，取hash值并前置，得到最终的存入redis的key

    val ifo = resource.get(final_key)
    if (StringUtils.isNotBlank(ifo)) {
      logRecord.ifo = ifo
    }
    logRecord
  }

  /**
    * 缓存各个小程序，各个模块的上线状态
    * @param sql_param  模块上线状态
    * @return
    */
  def cacheGrapUser(sql_param:String): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            s"""
              |select ak,$sql_param
              |from aldstat_grey_ak
            """.stripMargin)
          while (rs.next()) {
            val ak = rs.getString(1)
            val online_status = rs.getInt(2)
            if (ak != null && online_status != null)
              map.put(ak.toString, online_status.toString)
          }
      }
    }
    map
  }

  /**
    * 判断是灰度状态，还是上线状态
    * true:上线状态
    * false:灰度状态
    *
    * @return
    */
  def isOnline(): Boolean = {
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            """
              |SELECT online_status
              |FROM aldstat_online_status where online_status = 1
            """.stripMargin)
          if (rs.next()) {
            true
          } else {
            false
          }
      }
    }
  }

  /**
    * 分模块判断上线状态
    * true:上线状态
    * false:灰度状态
    * @param online_status
    * @return
    */
  def isOnline(online_status:String): Boolean = {
    use(getConnection()) { conn =>
      use(conn.createStatement()) {
        statement =>
          val rs = statement.executeQuery(
            s"""
              |SELECT $online_status
              |FROM aldstat_online_status where $online_status = 1
            """.stripMargin)
          if (rs.next()) {
            true
          } else {
            false
          }
      }
    }
  }

}
