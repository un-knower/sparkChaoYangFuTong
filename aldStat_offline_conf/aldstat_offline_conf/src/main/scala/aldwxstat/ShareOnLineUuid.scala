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
  * 用户分享
  * update by 游乐
  */
object ShareOnLineUuid {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")
    val sparkseesion = SparkSession
      .builder()
      //      .master("local")
      .config("spark.sql.shuffle.partitions", 200)
      .appName(this.getClass.getName)
      .getOrCreate()
    val args_limit_tmp = 1000
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
