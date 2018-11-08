package aldwxstat.aldlink

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/14. 
  */


class SingleAdLinkCollect {
  def linkSevenCollect(spark: SparkSession, numDay: String): Unit = {
    val day: String = ArgsTool.day
    val df = spark.sql("SELECT ak app_key,ag_ald_link_key,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak,ag_ald_link_key")

    df.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        s"""
          |insert into aldstat_${numDay}days_single_link(app_key,day,link_key,link_visitor_count)values(?,?,?,?)
          |ON DUPLICATE KEY UPDATE link_visitor_count=?
        """.stripMargin

      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val appKey = row.get(0).toString
        val linkId = row.get(1).toString
        val visitorCount = row.get(2).toString

        params.+=(Array[Any](appKey, day, linkId, visitorCount,visitorCount))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })

  }

  def mediaSevenCollect(spark: SparkSession, numDay: String): Unit = {
    val day: String = ArgsTool.day
    spark.sql("SELECT ak app_key,ag_ald_media_id,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak,ag_ald_media_id")
      .na.fill(0).na.fill("0").foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        s"""
          |insert into aldstat_${numDay}days_single_media(app_key,day,media_id,media_visitor_count)values(?,?,?,?)
          |ON DUPLICATE KEY UPDATE media_visitor_count=?
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val appKey = row.get(0).toString
        val mediaId = row.get(1).toString
        val visitorCount = row.get(2).toString

        params.+=(Array[Any](appKey, day, mediaId, visitorCount,visitorCount))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }

  def positionSevenCollect(spark: SparkSession, numDay: String): Unit = {
    val day: String = ArgsTool.day
    spark.sql("SELECT ak app_key,ag_ald_position_id,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak,ag_ald_position_id")
      .na.fill(0).na.fill("0").foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        s"""
          |insert into aldstat_${numDay}days_single_position(app_key,day,position_id,position_visitor_count)values(?,?,?,?)
          |ON DUPLICATE KEY UPDATE position_visitor_count=?
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val appKey = row.get(0).toString
        val positionId = row.get(1).toString
        val visitorCount = row.get(2).toString

        params.+=(Array[Any](appKey, day, positionId, visitorCount,visitorCount))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库

    })
  }


  def executeSingleSevenOrThirty(spark: SparkSession, numDays: String): Unit = {
    //    val logs = ArgsTool.getSevenOrThirtyDF(spark, DBConf.hdfsUrl, numDays)
    val logs = ArgsTool.getSevenOrThirtyDF(spark, ConfigurationUtil.getProperty("tongji.parquet"), numDays)

    //val logs: DataFrame = spark.read.parquet("/taiyang/link/20171214/etl-ald-log-2-2017121416")
    println(s"开始处理单个${numDays}天的数据")
    logs.filter("ev='page' and ag_ald_link_key!='null' and ag_ald_position_id!='null' and ag_ald_media_id!='null' and scene!='null'")
      .select("ak", "ag_ald_link_key", "ag_ald_media_id", "ag_ald_position_id", "uu", "hour", "at", "dr", "pp","scene")
      .createOrReplaceTempView("visitora_page")

    //    spark.sql(
    //      """
    //        |select ak,hour,del_link.link_key ag_ald_link_key,ag_ald_media_id,ag_ald_position_id,uu,at,dr,pp
    //        |from visitora_page vp
    //        |right join (select link_key from global_temp.ald_link_trace where is_del=0) del_link
    //        |on vp.ag_ald_link_key=del_link.link_key
    //      """.stripMargin).distinct()
    //      .createOrReplaceTempView("visitora_page")

    spark.sql(
      """
        |select trace.link_key ag_ald_link_key,trace.app_key ak,trace.media_id ag_ald_media_id,
        |case when scene in (1058,1035,1014,1038) then scene else '其它' end as ag_ald_position_id,
        |vp.hour,vp.uu,vp.at,vp.dr,vp.pp
        |from
        |(
        |select link_key,app_key,media_id
        |from global_temp.ald_link_trace
        |where is_del=0
        |) trace
        |left join visitora_page vp
        |on vp.ag_ald_link_key=trace.link_key
      """.stripMargin)
      .createOrReplaceTempView("visitora_page")

    spark.sqlContext.cacheTable("visitora_page")

    linkSevenCollect(spark, numDays)
    mediaSevenCollect(spark, numDays)
    positionSevenCollect(spark, numDays)

  }
}
