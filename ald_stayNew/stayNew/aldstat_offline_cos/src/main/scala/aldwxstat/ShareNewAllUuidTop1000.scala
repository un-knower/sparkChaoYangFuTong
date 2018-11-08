package aldwxstat

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * aldwxstat.Share_New_All
  * Created by 18510 on 2017/8/2.
  */
object ShareNewAllUuidTop1000 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")
    val sparkseesion = SparkSession
      .builder()
      //      .master("local")
      //      .config("spark.sql.shuffle.partitions", 200)
      .appName(this.getClass.getName)
      //.config("spark.sql.small.file.combine", "true")
      .getOrCreate()
    val args_limit_tmp = 10
    val yesterday = TimeUtil.processArgs(args)
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    //    val df_tmp = Aldstat_args_tool.analyze_args(args, sparkseesion, DBConf.hdfsUrl)
    val df_tmp = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))
    val rdd = df_tmp.toJSON.rdd.map(line => {
      val jsonLine = JSON.parseObject(line)
      val uu = jsonLine.get("uu")
      val path = jsonLine.get("path")
      val ak = jsonLine.get("ak")
      val src = jsonLine.get("wsr_query_ald_share_src")
      //初始化值，判断src是否为空，若不为空则去最后一个值
      var src1 = ""
      if (src != null) {
        src1 = src.toString.split("\\,")(src.toString.split("\\,").length - 1)
      }
      val tp = jsonLine.get("tp")
      val ct = jsonLine.get("ct")
      val ifo = jsonLine.get("ifo")
      val ev = jsonLine.get("ev")
      val hour = jsonLine.get("hour")
      val at = jsonLine.get("at")
      Row(uu, path, ak, src1, tp, ct, ifo, ev, hour, at)
    })
    val schema = StructType(List(
      StructField("uu", StringType, true),
      StructField("path", StringType, true),
      StructField("ak", StringType, true),
      StructField("src1", StringType, true),
      StructField("tp", StringType, true),
      StructField("ct", StringType, true),
      StructField("ifo", StringType, true),
      StructField("ev", StringType, true),
      StructField("hour", StringType, true),
      StructField("at", StringType, true)
    )
    )
    val df = sparkseesion.createDataFrame(rdd, schema)
    //创建临时表
    df.createTempView("share")
    sparkseesion.sqlContext.cacheTable("share")

    /**
      * 小时分享概况
      * update by 游乐
      * 2018-06-25
      */
    //分享次数和人数
    val share_hour = sparkseesion.sql("SELECT ak ,hour,COUNT(distinct uu) share_user_count,COUNT(at) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,hour")
    share_hour.createTempView("share_hour")

    //app新增
    val app_new_hour=sparkseesion.sql("select ak,hour,uu from share where ev='app' and ifo='true' and src1!=''")
    app_new_hour.createTempView("app_new_hour")

    //点击事件
    val share_click_hour=sparkseesion.sql("select ak,hour,uu from share WHERE ev='event' and path IS not NULL AND tp='ald_share_click'")
    share_click_hour.createTempView("share_click_hour")

    //分享新增
    val share_new_hour=sparkseesion.sql("select b.ak,b.hour,COUNT(DISTINCT b.uu) new_count from share_click_hour a inner join app_new_hour b on a.ak=b.ak and a.hour=b.hour and a.uu=b.uu GROUP BY b.ak,b.hour")
    share_new_hour.createTempView("share_new_hour")

    //分享打开数（回流量）
    val share_open_hour = sparkseesion.sql("SELECT ak ,hour,COUNT(at) share_open_count,count(DISTINCT uu) share_open_user_count FROM share WHERE ev='event' and tp='ald_share_click' and path IS not NULL GROUP BY ak,hour")
    share_open_hour.createTempView("share_open_hour")

    //分享回流比
    val share_reflux_ratio_hour = sparkseesion.sql("(SELECT a.ak,a.hour,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from share_hour a left join share_open_hour b on a.ak = b.ak and a.hour=b.hour) union (SELECT b.ak,b.hour,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from share_hour a right join share_open_hour b on a.ak = b.ak and a.hour=b.hour)")
    share_reflux_ratio_hour.createTempView("share_reflux_ratio_hour")

    //分享次数和新增
    val new_and_count_hour = sparkseesion.sql("(select a.ak,a.hour,a.share_user_count,a.share_count,b.new_count from share_hour a left join share_new_hour b on a.ak = b.ak and a.hour=b.hour) union (select b.ak,b.hour,a.share_user_count,a.share_count,b.new_count from share_hour a right join share_new_hour b on a.ak = b.ak and a.hour=b.hour)")
    new_and_count_hour.createTempView("new_and_count_hour")

    //分享打开人数次数join回流量
    val open_and_user_hour=sparkseesion.sql("(select a.ak,a.hour,b.share_open_count,b.share_open_user_count,a.share_reflux_ratio from share_reflux_ratio_hour a left join share_open_hour b on a.ak = b.ak and a.hour=b.hour) union (select b.ak,b.hour,b.share_open_count,b.share_open_user_count,a.share_reflux_ratio from share_reflux_ratio_hour a right join share_open_hour b on a.ak = b.ak and a.hour=b.hour)")
    open_and_user_hour.createTempView("open_and_user_hour")

    //汇总
    val result_hour = sparkseesion.sql("(select a.ak," + yesterday + " day,a.hour,b.share_user_count,b.new_count,b.share_count,a.share_open_count,a.share_open_user_count,a.share_reflux_ratio from open_and_user_hour a left join new_and_count_hour b on a.ak = b.ak and  a.hour=b.hour) union (select b.ak," + yesterday + " day,b.hour,b.share_user_count,b.new_count,b.share_count,a.share_open_count,a.share_open_user_count,a.share_reflux_ratio from open_and_user_hour a right join new_and_count_hour b on a.ak = b.ak and  a.hour=b.hour)").filter("hour != 'null'").na.fill(0)
    result_hour.createTempView("result_hour")
    sparkseesion.sqlContext.cacheTable("result_hour")

    result_hour.foreachPartition(rows => {
      dataframe2mysqlHour(rows)
    })
    /**
      * 一天分享概况
      * update by 游乐
      * 2018-06-01
      */

    //分享次数和人数
    val share_day = sparkseesion.sql("SELECT ak ,COUNT(distinct uu) share_user_count,COUNT(at) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak")
    share_day.createTempView("share_day")

    //分享带来新增
    val share_new_day = sparkseesion.sql("SELECT ak,COUNT(DISTINCT uu) new_count FROM share WHERE ev='event' and path IS not NULL AND tp='ald_share_click' and uu in (select uu from share where ev='app' and ifo='true') GROUP BY ak")
    share_new_day.createTempView("share_new_day")

    //分享打开数（回流量）
    val share_open_day = sparkseesion.sql("SELECT ak ,COUNT(at) share_open_count,count(DISTINCT uu) share_open_user_count FROM share WHERE ev='event' and tp='ald_share_click' and path IS not NULL GROUP BY ak")
    share_open_day.createTempView("share_open_day")

    //分享回流比
    val share_reflux_ratio_day = sparkseesion.sql("SELECT a.ak,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from share_day a left join share_open_day b on a.ak = b.ak")
    share_reflux_ratio_day.createTempView("share_reflux_ratio_day")

    //分享次数和新增
    val new_and_count_day = sparkseesion.sql("select a.ak,a.share_user_count,a.share_count,b.new_count from share_day a left join share_new_day b on a.ak = b.ak")
    new_and_count_day.createTempView("new_and_count_day")

    //分享打开人数次数join回流量
    val open_and_user_day = sparkseesion.sql("select a.ak,b.share_open_count,b.share_open_user_count,a.share_reflux_ratio from share_reflux_ratio_day a left join share_open_day b on a.ak = b.ak")
    open_and_user_day.createTempView("open_and_user_day")

    //汇总
    val result_day = sparkseesion.sql("select a.ak," + yesterday + " day,b.share_user_count,b.new_count,b.share_count,a.share_open_count,a.share_open_user_count,a.share_reflux_ratio from open_and_user_day a left join new_and_count_day b on a.ak = b.ak").na.fill(0)
    result_day.createTempView("result_day")
    sparkseesion.sqlContext.cacheTable("result_day")
    //入库
    result_day.foreachPartition(line => {
      dataframe2mysqlDay(line)
    })

    /**
      * 页面分享
      * update by 游乐
      * 2018-05-17
      */

    //分享次数和人数
    val page_share = sparkseesion.sql("SELECT ak app_key,path page_uri,COUNT(path) share_count,COUNT(DISTINCT uu) share_user_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,path")

    //分享带来的新增人数
    val page_click = sparkseesion.sql("SELECT ak app_key,COUNT(DISTINCT uu) new_count,path page_uri FROM share WHERE ev='event' and path IS not NULL AND tp='ald_share_click' and uu in (select uu from share where ev='app' and ifo='true') GROUP BY ak,path")
    page_click.createTempView("page_click")

    page_share.createTempView("page_share")

    //分享打开次数/分享打开人数
    val page_share_open_and_user_day = sparkseesion.sql("SELECT ak app_key,path page_uri,COUNT(path) share_open_count,count(DISTINCT uu) share_open_user_count FROM share WHERE ev='event' and tp='ald_share_click' and path IS not NULL GROUP BY ak,path")
    page_share_open_and_user_day.createTempView("page_share_open_and_user_day")

    //回流比
    val page_share_reflux_ratio_day = sparkseesion.sql("SELECT a.app_key app_key,a.page_uri page_uri,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from page_share a left join page_share_open_and_user_day b on a.app_key = b.app_key and a.page_uri = b.page_uri")
    page_share_reflux_ratio_day.createTempView("page_share_reflux_ratio1_day")

    val page_new_and_count_day = sparkseesion.sql("(select a.app_key,a.page_uri,a.share_count,a.share_user_count,b.new_count from page_share a left join page_click b on a.app_key = b.app_key and a.page_uri = b.page_uri) union (select b.app_key,b.page_uri,a.share_count,a.share_user_count,b.new_count from page_click b left join page_share a on a.app_key = b.app_key and a.page_uri = b.page_uri)")
    page_new_and_count_day.createTempView("page_new_and_count_day")

    val page_open_and_user_day = sparkseesion.sql("(select a.app_key,a.page_uri,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from page_share_open_and_user_day a left join page_share_reflux_ratio1_day b on a.app_key = b.app_key and a.page_uri = b.page_uri) union (select b.app_key,b.page_uri,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from page_share_reflux_ratio1_day b left join page_share_open_and_user_day a on a.app_key = b.app_key and a.page_uri = b.page_uri)")
    page_open_and_user_day.createTempView("page_open_and_user_day")

    val page_share_result = sparkseesion.sql("(select " + yesterday + " day, a.app_key,b.page_uri,a.share_count,a.share_user_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio from page_new_and_count_day a left join page_open_and_user_day b on a.app_key = b.app_key and a.page_uri = b.page_uri) union (select " + yesterday + " day,b.app_key,b.page_uri,a.share_count,a.share_user_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio from page_open_and_user_day b left join page_new_and_count_day a on a.app_key = b.app_key and a.page_uri = b.page_uri)").na.fill(0)

    page_share_result.createTempView("page_share_result")
    sparkseesion.sqlContext.cacheTable("page_share_result")

    //    页面的入库
    page_share_result.foreachPartition(line => {
      dataframe2mysqlPage(line)
    })

    /**
      * 用户分享
      * update by 游乐
      * 2018-06-25
      */

    //分享次数
    val user_share = sparkseesion.sql("SELECT ak app_key,uu sharer_uuid,COUNT(at) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,uu")
    user_share.createTempView("user_share")

    //统计出分享带来的新增人数
    val user_share_click = sparkseesion.sql("SELECT ak app_key,src1 sharer_uuid,COUNT(DISTINCT uu) new_count FROM share WHERE ev='event' and path IS not NULL AND tp='ald_share_click' and uu in (select uu from share where ev='app' and ifo='true' and src1 is not null) GROUP BY ak,src1").filter("sharer_uuid != ''")
    user_share_click.createTempView("user_share_click")

    //分享打开次数/分享打开人数
    val user_share_open_and_user_day = sparkseesion.sql("SELECT ak app_key,src1 sharer_uuid,COUNT(at) share_open_count,count(DISTINCT uu) share_open_user_count FROM share WHERE ev='event' and tp='ald_share_click' and path IS not NULL GROUP BY ak,src1")
    user_share_open_and_user_day.createTempView("user_share_open_and_user_day")

    //回流量
    val user_share_reflux_ratio_day = sparkseesion.sql("(SELECT a.app_key app_key,a.sharer_uuid sharer_uuid,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from user_share a left join user_share_open_and_user_day b on a.app_key = b.app_key and a.sharer_uuid=b.sharer_uuid) union (SELECT b.app_key app_key,b.sharer_uuid sharer_uuid,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from user_share a right join user_share_open_and_user_day b on a.app_key = b.app_key and a.sharer_uuid=b.sharer_uuid)")
    user_share_reflux_ratio_day.createTempView("user_share_reflux_ratio_day")

    val user_new_and_count_day = sparkseesion.sql("(select a.app_key,a.sharer_uuid,a.share_count,b.new_count from user_share a left join user_share_click b on a.app_key = b.app_key and a.sharer_uuid=b.sharer_uuid) union (select b.app_key,b.sharer_uuid,a.share_count,b.new_count from user_share a right join user_share_click b on a.app_key = b.app_key and a.sharer_uuid=b.sharer_uuid)")
    user_new_and_count_day.createTempView("user_new_and_count_day")

    val user_open_and_user_day = sparkseesion.sql("(select b.app_key,b.sharer_uuid,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from user_share_reflux_ratio_day b left join user_share_open_and_user_day a on a.app_key = b.app_key and a.sharer_uuid=b.sharer_uuid) union (select a.app_key,a.sharer_uuid,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from user_share_reflux_ratio_day b right join user_share_open_and_user_day a on a.app_key = b.app_key and a.sharer_uuid=b.sharer_uuid)")
    user_open_and_user_day.createTempView("user_open_and_user_day")

    val user_result_day = sparkseesion.sql("(select " + yesterday + " day, a.app_key,b.sharer_uuid,a.share_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio from user_new_and_count_day a left join user_open_and_user_day b on a.app_key = b.app_key and a.sharer_uuid = b.sharer_uuid) union (select " + yesterday + " day,b.app_key,b.sharer_uuid,a.share_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio from user_open_and_user_day b left join user_new_and_count_day a on a.app_key = b.app_key and a.sharer_uuid = b.sharer_uuid)").na.fill(0)
    user_result_day.createTempView("user_result_day")

    sparkseesion.sqlContext.cacheTable("user_result_day")

    user_result_day.foreachPartition(line => {
      dataframe2mysqlUser(line)
    })

    sparkseesion.close()
  }



  def dataframe2mysqlHour(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_hourly_share_summary (app_key,day,hour, share_user_count,new_count,share_count,share_open_count,share_open_user_count,share_reflux_ratio,update_at) values (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE share_user_count=?,new_count=?,share_count=?,share_open_count=?,share_open_user_count=?,share_reflux_ratio=?"

    iterator.foreach(r => {
      val app_key = r(0)
      val day=r(1)
      val hour=r(2)
      val share_user_count = r(3)
      val new_count = r(4)
      val share_count = r(5)
      val share_open_count = r(6)
      val share_open_user_count = r(7)
      val share_reflux_ratio = r(8)
      val update_at = UpDataTime
      params.+=(Array[Any](app_key,day,hour, share_user_count,new_count,share_count,share_open_count,share_open_user_count,share_reflux_ratio,update_at,share_user_count,new_count,share_count,share_open_count,share_open_user_count,share_reflux_ratio))
    })
    JdbcUtil.doBatch(sqlText, params) //批量入库
  }
  def dataframe2mysqlDay(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_daily_share_summary (app_key,day, share_user_count,new_count,share_count,share_open_count,share_open_user_count,share_reflux_ratio,update_at) values (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE share_user_count=?,new_count=?,share_count=?,share_open_count=?,share_open_user_count=?,share_reflux_ratio=?"

    iterator.foreach(r => {
      val app_key = r(0)
      val day=r(1)
      val share_user_count = r(2)
      val new_count = r(3)
      val share_count = r(4)
      val share_open_count = r(5)
      val share_open_user_count = r(6)
      val share_reflux_ratio = r(7)
      val update_at = UpDataTime
      params.+=(Array[Any](app_key,day, share_user_count,new_count,share_count,share_open_count,share_open_user_count,share_reflux_ratio,update_at,share_user_count,new_count,share_count,share_open_count,share_open_user_count,share_reflux_ratio))
    })
    JdbcUtil.doBatch(sqlText, params) //批量入库
  }

  def dataframe2mysqlPage(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_dailyshare_page (day,app_key,page_uri,share_user_count,share_open_user_count,share_open_count,share_reflux_ratio,share_count,new_count,update_at) values (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE share_count=?,new_count=?,update_at=?,share_open_user_count=?,share_open_count=?,share_reflux_ratio=?,share_user_count=?"

//          conn.setAutoCommit(false)
    iterator.foreach(r => {
      val date = r(0)
      val app_key = r(1)
      var page_uri=""
      if(r(2).toString.length>255){
        println(r(2).toString)
        page_uri = r(2).toString.substring(0,255)
      }else{
        page_uri=r(2).toString
      }
      val share_count = r(3)
      val share_user_count = r(4)
      val new_count = r(5)
      val share_open_count = r(6)
      val share_open_user_count = r(7)
      val share_reflux_ratio = r(8)

      val update_at = UpDataTime
      params.+=(Array[Any](date, app_key, page_uri, share_user_count, share_open_user_count, share_open_count, share_reflux_ratio, share_count, new_count, update_at, share_count, new_count, update_at, share_open_user_count, share_open_count, share_reflux_ratio, share_user_count))
    })
    JdbcUtil.doBatch(sqlText, params) //批量入库

  }

  def dataframe2mysqlUser(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_dailyshare_user (day,app_key,sharer_uuid,share_open_user_count,share_open_count,share_reflux_ratio," +
      s"share_count,new_count,update_at) values (?,?,?,?,?,?,?,?,?)" +
      s" ON DUPLICATE KEY UPDATE share_count=?,new_count=?,update_at=?,share_open_user_count=?,share_open_count=?,share_reflux_ratio=?"
    iterator.foreach(r => {
      val date = r(0)
      val app_key = r(1)
      val uu = r(2)
      val share_open_user_count = r(6)
      val share_open_count = r(5)
      val share_reflux_ratio = r(7)
      val share_count = r(3)
      val new_count = r(4)
      val update_at = UpDataTime

      params.+=(Array[Any](date, app_key, uu, share_open_user_count, share_open_count, share_reflux_ratio, share_count, new_count, update_at, share_count, new_count, update_at, share_open_user_count, share_open_count, share_reflux_ratio))
    })
    JdbcUtil.doBatch(sqlText, params)
  }
}
