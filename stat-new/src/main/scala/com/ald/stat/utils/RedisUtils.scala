package com.ald.stat.utils

import java.util
import java.util.List

import com.ald.stat.cache._
import com.ald.stat.utils.DBUtils.use
import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Protocol}

object RedisUtils {

  val baseRedisKey = "rt"

  /**
    *
    * 加锁
    *
    * @param jobClassName AnalysisTrend
    * @param redisCache   cache
    * @return
    */
  def lockJob(jobClassName: String, redisCache: ClientRedisCache): Boolean = {
    use(redisCache.getResource) {
      resource => {
        val lockKey = s"$baseRedisKey:lock:$jobClassName"
        resource.setnx(lockKey, "0") == 1l
      }
    }
  }

  /**
    * 释放lock
    *
    * @param jobClassName AnalysisTrend
    * @param redisCache
    */
  def freeJobLock(jobClassName: String, redisCache: ClientRedisCache): Unit = {
    val lockKey = s"$baseRedisKey:lock:$jobClassName"
    use(redisCache.getResource) {
      resource => {
        try {
          resource.del(lockKey)
        } catch {
          case e: Exception => {
            e.printStackTrace()
          }
        } finally {
          resource.del(lockKey)
        }
      }
    }
  }

  /**
    * 获取key
    *
    * @param d           date with yyyyMMdd
    * @param topic       topic
    * @param group       group
    * @param partitionId partitionId
    * @param mark        min|latest
    * @return
    */
  def buildOffsetKey(d: String, topic: String, group: String, partitionId: Int, mark: String): String = {
    s"$baseRedisKey:$d:offset:$topic:$partitionId:$group:$mark"
  }


  /**
    * 获取key
    *
    * @param d     date with yyyyMMdd
    * @param topic topic
    * @param group group
    * @param mark  donePartitions
    * @return
    */
  def buildDonePartitionsKey(d: String, topic: String, group: String, mark: String): String = {
    s"$baseRedisKey:$d:$topic:$group:$mark"
  }

  /**
    * clear key
    *
    * @param dateStr
    * @param jobKeyWord Trend|Share|Link
    * @param redisPrefix
    */
  def clearKeys(dateStr: String, jobKeyWord: String, redisPrefix: String): Unit = {
    val noCluster = ConfigUtils.getProperty(redisPrefix + "." + "client.redis.no.cluster")
    if (noCluster != null && noCluster == "true") {
      clearClientKeys(dateStr,jobKeyWord,redisPrefix)
    } else {
      clearClusterKeys(dateStr,jobKeyWord,redisPrefix)
    }
  }

  /**
    * 删除Client模式下的redis key
    * @param dateStr
    * @param jobKeyWord
    * @param redisPrefix
    */
  def clearClientKeys(dateStr: String, jobKeyWord: String, redisPrefix: String): Unit = {
    lazy val redisCache = CacheClientFactory.getInstances(redisPrefix).asInstanceOf[ClientRedisCache]
    val pattern = s"$baseRedisKey:$dateStr*$jobKeyWord*" //需要删除的key，包括pv和session相关的key
    println(pattern)
    use(redisCache.getResource) {
      resource => {
        val iter = resource.getAllShards.iterator()
        while (iter.hasNext) {
          val jedis = iter.next()
          val kIter = jedis.keys(pattern).iterator()
          while (kIter.hasNext) {
            val key = kIter.next()
            if (key.contains("offset")) {
              val v = jedis.get(key)
              println(s"$key->$v")
            }
            jedis.del(key)
          }
        }
      }
    }
    println("del Client keys over")
  }


  /**
    * 删除Cluster模式下的redis key
    * 注意：使用RedisCommand链接，每发出一条命令，都要相应的获取它的返回，不然会出现混乱
    * @param dateStr
    * @param jobKeyWord
    * @param redisPrefix
    */
  def clearClusterKeys(dateStr: String, jobKeyWord: String, redisPrefix: String): Unit = {
    val conn = RedisCommandFactory.getInstances(redisPrefix)
    val pattern = s"$baseRedisKey:$dateStr*$jobKeyWord*" //需要删除的key，包括pv和session相关的key
    println(pattern)
    var pwd = "redis.write.pool.password"
    if(!StringUtils.isEmpty(redisPrefix)){
      pwd = redisPrefix + "." + pwd
    }
    try {
      conn.sendCommand(Protocol.Command.AUTH, ConfigUtils.getProperty(pwd))
      val strStatus = conn.getStatusCodeReply //获取身份认证结果
      println("身份认证结果：" + strStatus)
      conn.sendCommand(Protocol.Command.CLUSTER, "nodes")
      val strNodeList = conn.getBulkReply //获取节点列表
      val nodeArr = strNodeList.split("\n")
      for(i <- 0 to nodeArr.length -1){
        val nodeInfo_arr = nodeArr(i).split(" ")
        if (nodeInfo_arr.length > 3) {
          val nodeId = nodeInfo_arr(0) //节点ID
          val nodeType = nodeInfo_arr(2) //节点类型 master | salve
          //获取master节点的key，并进行删除
          if (nodeType == "master" || nodeType == "myself,master") {
            conn.sendCommand(Protocol.Command.KEYS, pattern, nodeId)
            val keyList: util.List[String] = conn.getMultiBulkReply //获取该节点上匹配的key
            delKeys(keyList,redisPrefix)
          }
        }
      }
    } catch {
      case jce:JedisConnectionException => jce.printStackTrace()
    } finally {if (conn != null) conn.close()}
    println("del Cluster keys over")
  }

  /**
    * 删除redis key
    * @param keyList
    * @param redisPrefix
    */
  def delKeys( keyList: List[String],redisPrefix: String):Unit = {
    val redisCache = CacheClientFactory.getInstances(redisPrefix).asInstanceOf[ClientRedisCache]
    use(redisCache.getResource) {
      resource => {
        import scala.collection.JavaConversions._
        for (key <- keyList) {
          resource.del(key)
        }
      }
    }
  }

  /**
    * 清空集群模式的redis key，方便测试时使用
    * @param redisPrefix
    */
  def clearClusterKeys(redisPrefix: String): Unit = {
    val conn = RedisCommandFactory.getInstances(redisPrefix)
    val pattern = s"$baseRedisKey:*"
    println(pattern)
    var pwd = "redis.write.pool.password"
    if(!StringUtils.isEmpty(redisPrefix)){
      pwd = redisPrefix + "." + pwd
    }
    try {
      conn.sendCommand(Protocol.Command.AUTH, ConfigUtils.getProperty(pwd))
      val strStatus = conn.getStatusCodeReply //获取身份认证结果
      println("身份认证结果：" + strStatus)
      conn.sendCommand(Protocol.Command.CLUSTER, "nodes")
      val strNodeList = conn.getBulkReply //获取节点列表
      val nodeArr = strNodeList.split("\n")
      for(i <- 0 to nodeArr.length -1){
        val nodeInfo_arr = nodeArr(i).split(" ")
        if (nodeInfo_arr.length > 3) {
          val nodeId = nodeInfo_arr(0) //节点ID
          val nodeType = nodeInfo_arr(2) //节点类型 master | salve
          //获取master节点的key，并进行删除
          if (nodeType == "master" || nodeType == "myself,master") {
            conn.sendCommand(Protocol.Command.KEYS, pattern, nodeId)
            val keyList: util.List[String] = conn.getMultiBulkReply //获取该节点上匹配的key
            println("size: " + keyList.size())
            delKeys(keyList,redisPrefix)
          }
        }
      }
    } catch {
      case jce:JedisConnectionException => jce.printStackTrace()
    } finally {if (conn != null) conn.close()}
    println("del Cluster keys over")
  }

  def migrate(): Unit = {
    val oldRedisCache = CacheClientFactory.getInstances("old").asInstanceOf[ClientRedisCache]
    val targetRedisCache = CacheClientFactory.getInstances("default").asInstanceOf[ClientRedisCache]
    val targetResourse = targetRedisCache.getResource
    val pattern = "*offset*"
    use(oldRedisCache.getResource) {
      resource => {
        val iter = resource.getAllShards.iterator()
        while (iter.hasNext) {
          val jedis = iter.next()
          val kIter = jedis.keys(pattern).iterator()
          while (kIter.hasNext) {
            val key = kIter.next()
            val v = jedis.get(key)
            targetResourse.set(key, v)
          }
        }
      }
    }
    try {
      targetResourse.close()
    } catch {
      case _: Exception => {

      }
    }
  }

  /**
    * clear key
    *
    */
  def clearKeys(pattern: String, redisPrefix: String): Unit = {
    lazy val redisCache = CacheClientFactory.getInstances(redisPrefix).asInstanceOf[ClientRedisCache]
    println(pattern)
    use(redisCache.getResource) {
      resource => {
        val iter = resource.getAllShards.iterator()
        while (iter.hasNext) {
          val jedis = iter.next()
          val kIter = jedis.keys(pattern).iterator()
          while (kIter.hasNext) {
            jedis.del(kIter.next())
          }
        }
      }
    }
    println("del keys over")
  }

  /**
    * 构造pv uv 标记key
    *
    * @param dateStr
    * @param baseKey
    * @param key
    * @param moduleName
    * @param mark
    * @return
    */
  def buildPvUvMarkKey(dateStr: String, baseKey: String, key: String, moduleName: String, mark: String): String = {
    s"$baseKey:$dateStr:$moduleName:$key:$mark"
  }

  def buildSessionSumKey(dateStr: String, baseKey: String,name:String): String = {
    s"$baseKey:$dateStr:SUM:$name"
  }


  def main(args: Array[String]): Unit = {

    if(args.length == 2){
      clearKeys(args(1), args(0)) //删除client模式的key  参数列表：redisPrefix  pattern
    }else if(args.length == 3){
      //参数列表：dateStr pattern redisPrefix
      clearKeys(args(0), args(1),args(2)) //既可以删除client模式的key，也可以删除cluster模式的key
    }else if (args.length==1){
      clearClusterKeys(args(0))//一次性清空cluster模式下的key  参数列表：redisPrefix
    }

    //migrate()
  }

}
