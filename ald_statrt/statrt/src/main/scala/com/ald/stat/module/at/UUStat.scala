package com.ald.stat.module.at


import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.component.stat.UV
import com.ald.stat.log.LogRecord
import com.ald.stat.module.at.ATStat.{getDetailKey, getKey}
import com.ald.stat.utils.DBUtils.{getConnection, use}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
  * Created by spark01 on 6/5/18.
  */
object UUStat {

 val name ="EventUU"  //gcs:

  def statIncreaseCache[C <: LogRecord, K <: SubDimensionKey : ClassTag : Ordering, P <: DimensionKey : ClassTag : Ordering]
  (baseRedisKey: String, dateStr: String, statTrait: KeyParentTrait[C, K, P], logRecords: RDD[C], redisPrefix: Broadcast[String]): RDD[(P, (Long,String))]={


    logRecords.filter(str => StringUtils.isNotBlank(str.tp)).
      map(record => (statTrait.getKey(record))). //当时所有的uid已经去掉重复的
      mapPartitions( //现在判断是否要计数
      par => {

        val resource = CacheClientFactory.getInstances(redisPrefix.value).asInstanceOf[ClientRedisCache].getResource
        val baseKey = getKey(dateStr, baseRedisKey) //gcs:baseRedisKey+date+name => rt:event:20180502:AT
        val uidKey = getDetailKey(dateStr, baseRedisKey) //gcs:baseRedisKey 是用来形成Redis的key的
        val csts = ArrayBuffer[(P, (Long,String))]()
        par.foreach(record => {
          val key2 = statTrait.getKeyofDay(record).toString //当日只有一个

          val baseHashKey2 = statTrait.getBaseKey(record)
          val insertCount2 = resource.sadd(baseKey, key2) //检查插入的数量，如果是1，说明原系统没有，uid要增加数量
          val recordSplit = record.toString.split(":") //ak+time+tp
          val recordAk = recordSplit(0)
          val recordTp = recordSplit(2)
          if (1 == insertCount2 && StringUtils.isNotBlank(recordTp)) {
            try{
              var event_key: String = ""
              use(getConnection()) { conn =>
                use(conn.createStatement()) {
                  statement =>
                    var sql =
                      s"""
                         |select event_key
                         |from ald_event
                         |where app_key = "${recordAk}" and ev_id = "${recordTp}"
                    """.stripMargin
                    val rs = statement.executeQuery(
                      //gcs:select app_key,ev_id,event_key,ev_name
                      /*
                      *gcs:
                      *id       | app_key                          | ev_id             | event_key                        | ev_name                       | ev_status | ev_update_time      |
    +----------+----------------------------------+-------------------+----------------------------------+-------------------------------+-----------+---------------------+
    | 35030752 | 61b129ce5e318171f3cd9eec2b415c8d | ald_share_status  | c8fea4823abc0a86d36fe5f531a51f80 | ald_share_status              |         1 | 2018-06-03 18:03:03 |
    | 35030753 | 3d3db58fee367efd6af41c621c80468e | ald_share_click   | caebc4c737f785ff7bcbd07864660362 | ald_share_click               |         1 | 2018-06-03 15:02:13 |
    | 35030754 | 0fe03d20fc1e66599984ba2165852c6e | ald_error_message | 5db1c6b285d7f4590a58e20dd27fbfcf | ald_error_message             |         1 | 2018-03-22 14:29:15 |
    | 35030755 | 1d154aeefbb27bf5301328f76c8826c7 | ald_reachbottom   | f26f879f30fd8d9c2b7752394e41ffcb | 页面触底(阿拉丁默认)          |         1 | 2018-06-05 10:03:10 |
                      *
                      */
                      sql
                    )
                    if (rs.next()) {
                      event_key = rs.getString(1)
                    }
                }
              }
              try {
                if (StringUtils.isNotBlank(event_key)){
                  csts += ((baseHashKey2,(1l,event_key)))
                }
              }
              catch {
                case ex : Exception =>
                  null
                case ex : Error =>
                  null
              }
            }
            catch {
              case ex:Exception =>
                null
              case ex:Error =>
                null
            }
             }
        })

//        resource.expire(uidKey, Daily_Stat_Age)
        try {
          if (resource != null)
            resource.close()
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }
        csts.iterator
      }).reduceByKey((str1,str2) =>{
      (str1._1+str2._1,str1._2)
    })
    .mapPartitions(record =>{

      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val csts = ArrayBuffer[(P, (Long,String))]()
      record.foreach(str =>{

        var baseKeyRedisCount =s"${str._1}:eventAnalyseInnerCompute"
        var baseKeyFieldUU =s"${str._1}:uu"
        var finalUUCount = resource.hincrBy(baseKeyRedisCount,baseKeyFieldUU,str._2._1)

        csts += ((str._1,(finalUUCount,str._2._2)))

      })

      try {
        if (resource != null)
          resource.close()
      }
      catch {
        case t: Throwable => t.printStackTrace()
      }


      csts.iterator
    })
  }



  def getKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:$name"
  }

  def getDetailKey(dateStr: String, baseKey: String): String = {
    s"$baseKey:$dateStr:UID:$name"
  }
}
