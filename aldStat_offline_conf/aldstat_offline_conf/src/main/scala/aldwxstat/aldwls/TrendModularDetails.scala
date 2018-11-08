package aldwxstat.aldwls

import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/14.modified by clark 2018-08-02
  */
class TrendModularDetails(spark: SparkSession, dimension: String) extends PublicModularAnalysis {
  /**
    * 新用户数
    */
  def newUser(): Unit = {
    println("newUser")
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT p.ak app_key,p.hour,COUNT(DISTINCT p.uu) new_comer_count
           | FROM  page_view p JOIN app_view a ON p.uu=a.uu
           | WHERE a.ifo='true'
           | GROUP BY p.ak,p.hour"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
    } else {
      spark.sql(
        s"""SELECT p.ak app_key,COUNT(DISTINCT p.uu) new_comer_count
           | FROM page_view p JOIN app_view a ON p.uu=a.uu
           | WHERE a.ifo='true'
           | GROUP BY p.ak"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
    }

  }

  /**
    * 访问人数
    */
  def visitorCount(): Unit = {
    println("visitorCount")
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT ak app_key,hour,COUNT(DISTINCT uu) visitor_count
           |FROM page_view
           |GROUP BY ak,hour"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    } else {
      spark.sql(
        s"""SELECT ak app_key,COUNT(DISTINCT uu) visitor_count
           |FROM page_view
           |GROUP BY ak"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    }
  }

  /**
    * 打开次数
    * 你看原来的算法是写死的。它不能灵活地将视图传进来。
    * 按照刚才的那种方法进行更改，就可以灵活地将“维度视图”传进来了
    */
  def openCount(): Unit = {
    println("opencount")
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT ak app_key,hour,COUNT(DISTINCT at) open_count
           |FROM page_view
           |GROUP BY ak,hour"""
          .stripMargin)
        .createOrReplaceTempView("open_count")
    }else {
      spark.sql(
        s"""SELECT ak app_key,COUNT(DISTINCT at) open_count
           |FROM page_view
           |GROUP BY ak"""
          .stripMargin)
        .createOrReplaceTempView("open_count")
    }
  }



  /**
    * 访问次数，页面总访问量
    */
  def totalPageCount(): Unit = {
    println("totalPageCount")
    if (dimension == "hour") {
      spark.sql(
        s"""
           |SELECT ak app_key,hour,COUNT(pp) total_page_count
           |FROM page_view
           |GROUP BY hour,ak"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }else {
      spark.sql(
        s"""
           |SELECT ak app_key,COUNT(pp) total_page_count
           |FROM page_view
           |GROUP BY ak"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }

  }

  /**
    * 总停留时长
    */
  def totalStayTime(): Unit = {
    println("totalStayTime")
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT ak app_key,hour,sum(time) total_stay_time
           |FROM (SELECT ak,hour,max(dr/1000) time
           |FROM (SELECT ak,hour,dr,at from app_view UNION SELECT ak,hour,dr,at from page_view) GROUP BY ak,at,hour
           |) group by hour,ak"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")

    }else {
      spark.sql(
        s"""SELECT ak app_key,sum(time) total_stay_time
           |FROM (SELECT ak,max(dr/1000) time
           |FROM (SELECT ak,dr,at from app_view UNION SELECT ak,dr,at from page_view) GROUP BY ak,at
           |) group by ak"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")
    }
  }

  /**
    * 次均停留时长
    */
  def secondaryAvgStayTime(): Unit = {
    println("secondaryAvgStayTime")
    if (dimension == "hour") {
      spark.sql(
        """
          |SELECT oc.app_key,oc.hour,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
          |FROM open_count oc
          |left join total_stay_time tst
          |on oc.app_key=tst.app_key and oc.hour=tst.hour
        """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    }else {
      spark.sql(
        """
          |SELECT oc.app_key,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
          |FROM open_count oc
          |left join total_stay_time tst
          |on oc.app_key=tst.app_key
        """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    }

  }

  /**
    * 跳出页个数
    */
  def visitPageOnce(): Unit = {
    println("visitPageOnce")
    if (dimension == "hour") {
      spark.sql(//ak，at，所有page的访问次数=1
        s"""SELECT tmp.ak app_key,COUNT(at) one_page_count,hour FROM
           | (SELECT ak ,at,COUNT(distinct pp) cp,hour
           | FROM page_view WHERE pp!='null'
           | GROUP BY ak,at,hour
           | HAVING cp=1) tmp GROUP BY app_key,hour"""
          .stripMargin)
        .createOrReplaceTempView("visit_page_once")
    }else {
      spark.sql(//ak，at，所有page的访问次数=1
        s""" SELECT tmp.ak app_key,COUNT(at) one_page_count FROM
           | (SELECT ak ,at,COUNT(distinct pp) cp
           | FROM page_view WHERE pp!='null'
           | GROUP BY ak,at
           | HAVING cp=1) tmp GROUP BY app_key
           | """.stripMargin)
        .createOrReplaceTempView("visit_page_once")
    }
  }

  /**
    * 页面跳出率,每个ak的跳出页个数/总的页面访问量
    */
  def bounceRate(): Unit = {
    println("bounceRate")
    if (dimension == "hour") {
      spark.sql(
        """
          |select tpc.app_key app_key,tpc.hour,cast(vpo.one_page_count/tpc.open_count as float) bounce_rate
          |from open_count tpc
          |left join visit_page_once vpo
          |on tpc.app_key = vpo.app_key and tpc.hour = vpo.hour
        """.stripMargin).createOrReplaceTempView("bounce_rate")
    }else {
      spark.sql(
        """
          |select tpc.app_key app_key,cast(vpo.one_page_count/tpc.open_count as float) bounce_rate
          |from open_count tpc
          |left join visit_page_once vpo
          |on tpc.app_key = vpo.app_key
        """.stripMargin).createOrReplaceTempView("bounce_rate")
    }

  }

  def shareCount(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        """SELECT ak app_key,hour,COUNT(path) share_count FROM visitor_event where ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,hour
        """).createOrReplaceTempView("share_count")
    }else {
      spark.sql(
        """SELECT ak app_key,COUNT(path) share_count FROM visitor_event where ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak
        """).createOrReplaceTempView("share_count")
    }

  }

}
