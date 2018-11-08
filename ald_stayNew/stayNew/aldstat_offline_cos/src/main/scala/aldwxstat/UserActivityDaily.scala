package aldwxstat

import aldwxutils.{JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2017/8/22. Modified 2018-05-16 clark
  * 用户活跃度
  */
object UserActivityDaily {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val yesterday = TimeUtil.processArgs(args)
    val yesterday00 = TimeUtil.str2Long(yesterday) / 1000
    val sevenDaysAgo = yesterday00 - (86400 * 6).toLong
    val thirtyDaysAgo = yesterday00 - (86400 * 29).toLong

    val sixDays = for (stamp <- sevenDaysAgo to yesterday00 if (stamp % 86400 == 0)) yield TimeUtil.long2Str(stamp * 1000)
    val twenty3Days = for (stamp <- thirtyDaysAgo to sevenDaysAgo if (stamp % 86400 == 0)) yield TimeUtil.long2Str(stamp * 1000)


    val spark = SparkSession.builder().appName(this.getClass.getName) //.master("local[*]")
      .getOrCreate()

    val oneFile = spark.read.parquet(s"cosn://aldwxlogbackup/log_parquet/*$yesterday*").filter("ev='app'").select("ak", "uu").distinct()
    oneFile.createTempView("oneView")
    spark.sql("SELECT ak app_key,COUNT(uu) dau FROM oneView GROUP BY ak").createTempView("yesterday")


    val sixFiles = sixDays.map(day => {
      spark.read.parquet(s"cosn://aldwxlogbackup/log_parquet/*$day*").filter("ev='app'").select("ak", "uu").distinct()
    })
    val sevenFiles=sixFiles.reduce((df1: DataFrame, df2: DataFrame) => {
      df1.union(df2).distinct()
    }).union(oneFile).distinct()
    sevenFiles.createTempView("sevenView")
    spark.sql("SELECT ak app_key,COUNT(uu) wau FROM sevenView GROUP BY ak").createTempView("week")


    val twenty3Files = twenty3Days.map(day => {
      spark.read.parquet(s"cosn://aldwxlogbackup/log_parquet/*$day*").filter("ev='app'").select("ak", "uu").distinct()
    })
    val thirtyFiles=twenty3Files.reduce((df1: DataFrame, df2: DataFrame) => {
      df1.union(df2).distinct()
    }).union(sevenFiles).distinct()
    thirtyFiles.createTempView("thirtyView")
    spark.sql("SELECT ak app_key,COUNT(uu) mau FROM thirtyView GROUP BY ak").createTempView("month")

    val result = spark.sql("SELECT month.app_key," + yesterday + " day, yesterday.dau,week.wau, " +
      "(cast(yesterday.dau as double) / cast(week.wau as double)) AS dau_wau_ratio ," +
      "month.mau," +
      "(cast(yesterday.dau as double) / cast(month.mau as double)) AS dau_mau_ratio , " +
      "NOW() as update_at FROM month LEFT JOIN week ON month.app_key =week.app_key LEFT JOIN " +
      "yesterday ON month.app_key =yesterday.app_key").na.fill(0)

    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_user_activity (app_key,day,dau,wau, dau_wau_ratio," +
        s" mau,dau_mau_ratio,update_at) " +
        s" values (?,?,?,?,?,?,?,?) ON " +
        s" DUPLICATE KEY UPDATE dau=?, wau=?, dau_wau_ratio=?,mau=?,dau_mau_ratio=?"

      rows.foreach(r => {
        val app_key = r.get(0)
        val day = r.get(1)
        val dau = r.get(2)
        val wau = r.get(3)
        val dau_wau_ratio = r.get(4)
        val mau = r.get(5)
        val dau_mau_ratio = r.get(6)
        val update_at = r.get(7)

        params.+=(Array[Any](app_key, day, dau, wau, dau_wau_ratio, mau, dau_mau_ratio, update_at, dau, wau, dau_wau_ratio, mau, dau_mau_ratio))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}
