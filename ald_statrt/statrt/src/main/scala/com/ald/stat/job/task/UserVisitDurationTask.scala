package com.ald.stat.job.task

import java.sql.Timestamp
import java.time.LocalDateTime

import com.ald.stat.cache.{CacheClientFactory, ClientRedisCache}
import com.ald.stat.component.dimension.{DimensionKey,DimensionKeyExtend}
import com.ald.stat.log.LogRecordExtendSS
import com.ald.stat.utils.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.ald.stat.module.uv.UVDurationStat
import com.ald.stat.component.dimension.depthAndDuration.{DailyUVDurationATSubDimensionKeyExtend, DailyUVDurationUUSubSimensionKeyExtend}

/**
  * Created by spark01 on 6/8/18.
  */
object UserVisitDurationTask  extends TaskTrait{

  def dailyStat(baseRedisKey: String, taskId: String, dateStr: String, logRecordRdd: RDD[LogRecordExtendSS], kafkaProducer: Broadcast[KafkaSink[String, String]], redisPrefix: Broadcast[String]): Unit ={


    val uv_userVisitDuration = UVDurationStat.statIncreaseCacheExtendUU(baseRedisKey,dateStr,DailyUVDurationUUSubSimensionKeyExtend,logRecordRdd,redisPrefix)
    val at_userVisitDuration = UVDurationStat.statIncreaseCacheExtendAT(baseRedisKey,dateStr,DailyUVDurationATSubDimensionKeyExtend,logRecordRdd,redisPrefix)
    val final_userVisitDuration = uv_userVisitDuration.join(at_userVisitDuration)   //gcs:uv在前，at在后

    writeKafka(kafkaProducer,final_userVisitDuration)
  }


  def writeKafka[P <: DimensionKeyExtend : Ordering]
  ( kafkaProducer: Broadcast[KafkaSink[String, String]], rdd:  RDD[(P, ((Long, (String, String)), (Long, (String, String))))]) ={


    rdd.foreachPartition(par => {
      val resource = CacheClientFactory.getInstances().asInstanceOf[ClientRedisCache].getResource
      val kafka: KafkaSink[String, String] = kafkaProducer.value
      par.foreach(row => {
        //从DimensionKey中获取
        val splits = row._1.toString.split(":")
        val app_key = splits(0)
        val day = splits(1)
        var visit_duration = splits(2) //gcs:访问时长
        var visitor_count = row._2._1._1 //gcs:当前访问时长下的访问人数
        var visitor_ratio =0f   //gcs:当前访问深度的访问人数/总的访问人数 比值
        var open_count = row._2._2._1  //gcs:当前访问深度下的访问次数
        var open_ratio = 0f  //gcs:当前深度下的访问次数/总的访问次数。比值
        val updateAt = Timestamp.valueOf(LocalDateTime.now())

        var baseKeyUu = row._2._1._2._1  //gcs：在redis中进行查询的baseKey
        var bashHashKeyUu = row._2._1._2._2  //gcs:在redis中进行查询的key的field值

        println(s"------------------------3-------------------------")
        var sumUuCount = resource.hget(baseKeyUu,bashHashKeyUu) //gcs:
        println(s"------------------------${sumUuCount}-------------------------")

        var baseKeyAt = row._2._2._2._1
        var bashHashKeyAt = row._2._2._2._2
        println(s"------------------------4-------------------------")
        var sumAtCount = resource.hget(baseKeyAt,bashHashKeyAt)
        println(s"------------------------${sumAtCount}-------------------------")


        if (0 != sumUuCount){
          visitor_ratio =visitor_count.toFloat / sumUuCount.toFloat
        }

        if (0 !=sumAtCount){
          open_ratio = open_count.toFloat / sumAtCount.toFloat
        }


        try {
          if (resource != null)
            resource.close()
        }
        catch {
          case t: Throwable => t.printStackTrace()
        }

        val sqlInsertOrUpdate =
          s"""
             |insert into aldstat_visit_duration
             |(
             |    app_key,
             |    day,
             |    visit_duration,
             |    visitor_count,
             |    visitor_ratio,
             |    open_count,
             |    open_ratio,
             |    update_at
             |)
             |values(
             |    "$app_key",
             |    "$day",
             |    "$visit_duration",
             |    "$visitor_count",
             |    "$visitor_ratio",
             |    "$open_count",
             |    "$open_ratio",
             |    "$updateAt",
             |)
             |on DUPLICATE KEY UPDATE
             |app_key="$app_key",
             |day="$day",
             |visit_duration="$visit_duration",
             |visitor_count="$visitor_count",
             |visitor_ratio="$visitor_ratio",
             |open_count="$open_count",
             |open_ratio="$open_ratio",
             |update_at="$updateAt"
            """.stripMargin

        kafka.send("mysql_test", sqlInsertOrUpdate)

      })
    })


  }

}
