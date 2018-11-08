package aldwxstat.aldwls

import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/14. 
  */
class PublicHourModularDetails(spark: SparkSession, dimension: String) extends PublicModularAnalysis {
  /**
    * 新用户数
    */
  def newUser(): Unit = {
    if (dimension == "ww_wh") {
      spark.sql(
        s"""SELECT p.ak app_key,p.${dimension} tmp_sum,COUNT(DISTINCT p.uu) new_comer_count
           | FROM page_view p JOIN app_view a ON p.uu=a.uu
           | WHERE a.ifo='true'
           | GROUP BY p.ak,p.${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
//      spark.sql("select * from new_user_daily").show()
    }else {
      spark.sql(
        s"""SELECT p.ak app_key,p.hour,p.${dimension} tmp_sum,COUNT(DISTINCT p.uu) new_comer_count
           | FROM page_view p JOIN app_view a ON p.uu=a.uu
           | WHERE a.ifo='true'
           | GROUP BY p.ak,p.hour,p.${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
    }
  }

  /**
    * 访问人数
    */
  def visitorCount(): Unit = {
    if (dimension == "ww_wh") {
      spark.sql(
        s"""SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT uu) visitor_count
           |FROM page_view
           |GROUP BY ak,concat(ww,'*',wh)"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    } else{
      spark.sql(
        s"""SELECT ak app_key,hour,${dimension} tmp_sum,COUNT(DISTINCT uu) visitor_count
           |FROM page_view
           |GROUP BY ak,hour,${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    }
  }

  /**
    * 打开次数
    */
  def openCount(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(
        s"""SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT at) open_count
           |FROM page_view
           |GROUP BY ak,pixel"""
          .stripMargin)
        .createOrReplaceTempView("open_count")
    }else{
      spark.sql(
        s"""| SELECT ak app_key, ${dimension} tmp_sum,COUNT(DISTINCT at) open_count,hour
            | FROM page_view
            | GROUP BY ak,${dimension},hour""".stripMargin)
        .createOrReplaceTempView("open_count")
    }
  }

  /**
    * 访问次数，页面总访问量
    */
  def totalPageCount(): Unit = {
    if (dimension == "ww_wh") {
      spark.sql(
        s"""
           |SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(pp) total_page_count
           |FROM page_view
           |GROUP BY ak,concat(ww,'*',wh)"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }else {
      spark.sql(
        s"""
           |SELECT ak app_key,hour,${dimension} tmp_sum,COUNT(pp) total_page_count
           |FROM page_view
           |GROUP BY ak,hour,${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }

  }

  /**
    * 总停留时长
    */
  def totalStayTime(): Unit = {
    if (dimension == "ww_wh") {
      spark.sql(
        s"""
           |SELECT ak app_key,tmp_sum,sum(time) total_stay_time
           |FROM (
           |SELECT ak,concat(ww,'*',wh) tmp_sum,max(dr/1000) time
           |FROM (SELECT ak,dr,at,ww,wh from app_view UNION SELECT ak,hour,dr,at,ww,wh from page_view) GROUP BY ak,at,concat(ww,'*',wh)
           |)group by tmp_sum,ak
           |"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")
    }else {
      spark.sql(
        s"""SELECT ak app_key,hour,${dimension} tmp_sum,sum(time) total_stay_time
           |FROM (
           |SELECT ak,hour,${dimension},max(dr/1000) time
           |FROM (SELECT ak,dr,at,hour,$dimension from app_view UNION SELECT ak,dr,at,hour,$dimension from page_view) GROUP BY ak,at,hour,${dimension}
           |) group by hour,${dimension},ak"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")
    }
  }

  /**
    * 次均停留时长
    */
  def secondaryAvgStayTime(): Unit = {
    spark.sql(
      """
        |SELECT oc.app_key,oc.tmp_sum,oc.hour,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.tmp_sum=tst.tmp_sum and oc.hour=tst.hour
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
  }

  /**
    * 跳出页个数
    */
  def visitPageOnce(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(//|ak，at，所有page的访问次数=1|
        s""" SELECT tmp.ak app_key,COUNT(at) one_page_count,Pixel tmp_sum,hour FROM
           | (SELECT ak ,at,COUNT(distinct pp) cp,concat(ww,'*',wh) Pixel,hour
           | FROM page_view WHERE pp!='null' and pp!=''
           | GROUP BY ak,at,concat(ww,'*',wh),hour
           | HAVING cp=1) tmp GROUP BY app_key,Pixel,hour
           | """.stripMargin)
        .createOrReplaceTempView("visit_page_once")
    }else {
      spark.sql(//|ak，at，所有page的访问次数=1|
        s""" SELECT tmp.ak app_key,COUNT(at) one_page_count,$dimension tmp_sum,hour FROM
           | (SELECT ak ,at,COUNT(distinct pp) cp,$dimension,hour
           | FROM page_view WHERE pp!='null' and pp!=''
           | GROUP BY ak,at,$dimension,hour
           | HAVING cp=1) tmp GROUP BY app_key,$dimension,hour
           | """.stripMargin)
        .createOrReplaceTempView("visit_page_once")
    }
  }

  /**
    * 页面跳出率,每个ak的跳出页个数/总的页面访问量
    */
  def bounceRate(): Unit = {
    spark.sql(
      """
        |select tpc.app_key app_key,tpc.hour,tpc.tmp_sum,cast(vpo.one_page_count/tpc.total_page_count as float) bounce_rate
        |from total_page_count tpc
        |left join visit_page_once vpo
        |on tpc.app_key = vpo.app_key and tpc.tmp_sum=vpo.tmp_sum and tpc.hour=vpo.hour
      """.stripMargin).createOrReplaceTempView("bounce_rate")
  }

  def shareCount(): Unit = {
  }
}