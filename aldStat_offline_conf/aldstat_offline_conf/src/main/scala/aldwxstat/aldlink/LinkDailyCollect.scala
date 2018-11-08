
package aldwxstat.aldlink

import java.sql.Timestamp

import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/14.
  * 外链每日汇总
  */
class LinkDailyCollect(spark: SparkSession) extends LinkAnalysis {


  //点击量(click_count)
  def clickCount(): Unit = {
    println("计算点击量")
    spark.sql("SELECT ak app_key,COUNT(pp) click_count FROM visitora_page GROUP BY ak")
      .createOrReplaceTempView("click_count")
  }



  //新用户数
  def newUser(): Unit = {
    println("计算新用户数...")
    //spark.sql("SELECT ak,uu FROM visitora_app WHERE ifo='true' GROUP BY ak,uu").createOrReplaceTempView("nu_app_daily")
    //spark.sql("SELECT ak,uu FROM visitora WHERE ev = 'page' and ag_ald_link_key!=null and ag_ald_position_id!=null and ag_ald_media_id!=null GROUP BY ak,uu")
    spark.sql("SELECT ak,uu FROM visitora_page GROUP BY ak,uu")
      .createOrReplaceTempView("nu_page_daily")
    spark.sql(
      """select ak app_key,count(distinct uu) new_comer_count
        |from (select page.ak ak,page.uu from nu_app_daily app inner join nu_page_daily page on page.ak=app.ak and page.uu=app.uu)
        |group by ak
      """.stripMargin).createOrReplaceTempView("new_user_daily")
    //spark.sql("select * from new_user_daily where app_key='64a7f2b033fb593a8598bbc48c3b8486'").show()

  }

  //访问人数(visitor_count)
  def visitorCount(): Unit = {
    println("计算访问人数...")
    spark.sql("SELECT ak app_key,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak")
      .createOrReplaceTempView("visitor_count")
    //spark.sql("select * from visitor_count where app_key='64a7f2b033fb593a8598bbc48c3b8486'").show()
  }

  //授权用户(authuser_count)
  def atSession2(): Unit = {
    println("计算授权人数...")
    spark.sql("SELECT  ak, at  FROM logs where ak!='null'and at!='null' and ag_ald_link_key!='null'and ifo='true' and (scene!='1020'or scene!='1089'  or scene!='1001' or scene!='1023' or scene!='1069' or scene!='1024' or scene!='1006') GROUP BY ak,at")
      .createOrReplaceTempView("at_session2")
  }


  def authuserCount(): Unit = {
    spark.sql("SELECT a.ak app_key,count(DISTINCT b.img) authuser_count from at_session2 a left join (select ak ,at,img from logs where img!='' and img!='null') b on a.ak=b.ak and a.at=b.at GROUP BY a.ak")

      .createOrReplaceTempView("authuser_count")

  }



  //打开次数(open_count)
  def openCount(): Unit = {
    println("计算打开次数")
    spark.sql("SELECT ak app_key,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak")
      .createOrReplaceTempView("open_count")
    //spark.sql("select * from open_count where app_key='64a7f2b033fb593a8598bbc48c3b8486'").show()
  }

  //访问次数，页面总访问量

  def atSession(): Unit = {
    println("计算访问次数")
    //spark.sql("SELECT DISTINCT ak, at FROM logs where ev='page' GROUP BY ak")
    spark.sql("SELECT  ak, at  FROM logs where ev='page' and ak!='null'and at!='null' and ag_ald_link_key!='null'and(scene!='1020'or scene!='1089'  or scene!='1001' or scene!='1023' or scene!='1069' or scene!='1024' or scene!='1006') GROUP BY ak,at")
      .createOrReplaceTempView("at_session")
  }


  def totalPageCount(): Unit = {
    spark.sql("SELECT a.ak app_key,count(b.pp) total_page_count from at_session a left join (select ak ,at,pp from logs where ev='page') b on a.ak=b.ak and a.at=b.at GROUP BY a.ak")
      .createOrReplaceTempView("total_page_count")
    //spark.sql("select * from total_page_count where app_key='64a7f2b033fb593a8598bbc48c3b8486'").show(30)
  }




  //总停留时长
  def totalStayTime(): Unit = {
    println("计算停留时间")
    spark.sql(
      """
        |select app_key,sum(dr) total_stay_time from
        |(
        |select w.app_key,case when w.v >='7.0.0' then w.dr/1000 else w.dr end as dr  from
        |(
        |select page.ak app_key,max(page.dr) as dr,page.v
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at
        |group by app_key,page.at,page.v
        |) w
        |)
        |group by app_key
      """.stripMargin).createOrReplaceTempView("total_stay_time")
  }

  //次均停留时长
  def secondaryAvgStayTime(): Unit = {
    println("计算次均停留时长")
    spark.sql(
      """
        |SELECT oc.app_key,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
  }

  //跳出页个数
  def visitPageOnce(): Unit = {
    println("计算跳出页个数")
    spark.sql(//|ak，at，所有page的访问次数=1|
      s"""
         |select tmp.ak app_key,sum(tmp.cp) once_page
         |from (SELECT ak,at,COUNT(pp) cp FROM visitora_page GROUP BY ak,at,pp) tmp
         |group by tmp.ak,tmp.at
       """.stripMargin).createOrReplaceTempView("visit_page_sum_once")

    spark.sql(//|ak，at，所有page的访问次数=1|
      s"""
         |select app_key,sum(once_page) one_page_count
         |from visit_page_sum_once
         |where once_page=1
         |group by app_key
       """.stripMargin).createOrReplaceTempView("visit_page_once")
    //spark.sql("select * from visit_page_once").show()
  }

  //页面跳出率,每个ak的跳出页个数/总的页面访问量
  def bounceRate(): Unit = {
    println("计算页面跳出率")
    spark.sql(
      """
        |select oc.app_key app_key,cast(vpo.one_page_count/oc.open_count as float) bounce_rate
        |from open_count oc
        |left join visit_page_once vpo
        |on oc.app_key = vpo.app_key
      """.stripMargin).createOrReplaceTempView("bounce_rate")
  }



  //外链每日详情入库
  def insert2db(): Unit = {
    println("计算外链每日详情入库")
    //val yesterday = new DateTimeUtil().getUdfDate(-1,"yyyy-MM-dd")//LocalDate.now().plusDays(-1).format(dtf)
    val day = ArgsTool.day

    val linkDailyDetail = spark.sql(
      """
        |select vc.app_key,vc.visitor_count,ac.authuser_count,oc.open_count,tpc.total_page_count,cc.click_count,nu.new_comer_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,vpo.one_page_count,bounce.bounce_rate
        |from visitor_count vc
        |left join authuser_count ac
        |on vc.app_key=ac.app_key
        |left join open_count oc
        |on vc.app_key=oc.app_key
        |left join total_page_count tpc
        |on vc.app_key=tpc.app_key
        |left join click_count cc
        |on vc.app_key=cc.app_key
        |left join new_user_daily nu
        |on vc.app_key=nu.app_key
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key
        |left join visit_page_once vpo
        |on vc.app_key=vpo.app_key
        |left join bounce_rate bounce
        |on vc.app_key=bounce.app_key
      """.stripMargin) //.show()
    //linkDailyDetail.show()
    println("计算完成准备入库...")
    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      //      val sqlText =
      //        """
      //          | replace into aldstat_link_summary(app_key,day,link_visitor_count,link_open_count,link_page_count,link_newer_for_app,total_stay_time,secondary_stay_time,one_page_count,bounce_rate,update_at)
      //          | values(?,?,?,?,?,?,?,?,?,?,?)
      //        """.stripMargin //准备sql模板
      val sqlText =
      """
        | insert into aldstat_link_summary(app_key,day,link_visitor_count,link_authuser_count,link_open_count,link_page_count,link_click_count,
        | link_newer_for_app,total_stay_time,secondary_stay_time,one_page_count,bounce_rate,update_at)
        | values(?,?,?,?,?,?,?,?,?,?,?,?,?)
        | ON DUPLICATE KEY UPDATE
        | link_visitor_count=?,link_authuser_count=?,link_open_count=?,link_page_count=?,link_click_count=?,link_newer_for_app=?,total_stay_time=?,
        | secondary_stay_time=?,one_page_count=?,bounce_rate=?,update_at=?
      """.stripMargin
      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val visitor_count = row.get(1).toString
        val authuser_count =row.get(2).toString
        val open_count = row.get(3).toString
        val total_page_count = row.get(4).toString
        val click_count =row.get(5).toString
        val new_user_daily = row.get(6).toString
        val total_stay_time = row.get(7).toString
        val secondary_avg_stay_time = row.get(8).toString
        val visit_page_once = row.get(9).toString
        val bounce_rate = row.get(10).toString

        params.+=(Array[Any](app_key, day, visitor_count,authuser_count, open_count, total_page_count,click_count, new_user_daily, total_stay_time,
          secondary_avg_stay_time, visit_page_once, bounce_rate, update_at,
          visitor_count,authuser_count,open_count,total_page_count,click_count,new_user_daily, total_stay_time,secondary_avg_stay_time,visit_page_once,bounce_rate,update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库

    })
    //spark.close()
  }
}

