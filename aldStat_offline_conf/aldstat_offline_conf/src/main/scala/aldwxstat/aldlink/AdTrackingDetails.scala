package aldwxstat.aldlink

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/13.
  * 1
  */

object AdTrackingDetails {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AdTrackingDetails")
      .getOrCreate()

    println("创建全局管理表ald_link_trace")
    JdbcUtil.readFromMysql(spark, "ald_link_trace").createGlobalTempView("ald_link_trace")
    spark.sqlContext.cacheTable("global_temp.ald_link_trace")

    execute(args, spark)

    spark.close()
  }

  def execute(args: Array[String], spark: SparkSession): Unit = {
    ArgsTool.analysisArgs(args)
    if (args.length == 0) {
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      fillDailyData(spark)
      //      fillSevenOrThirtyData(spark, "7")
      //      fillSevenOrThirtyData(spark, "30")
    } else if (ArgsTool.du != "" && ArgsTool.day != "") {
      println("补" + ArgsTool.du + ArgsTool.hour + "的数据")
      //补指定日期的7天或30天数据（包括指定的日期）
      fillSevenOrThirtyData(spark, ArgsTool.du)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      //补某一天数据（包括指定的日期）
      fillDailyData(spark)
    }

  }


  //补每日数据
  def fillDailyData(spark: SparkSession): Unit = {
    //    val logs = ArgsTool.getDailyDataFrame(spark, DBConf.hdfsUrl)
    val logs = ArgsTool.getDailyDataFrame(spark, ConfigurationUtil.getProperty("tongji.parquet"))

    //val logs = spark.read.parquet("/taiyang/link/20171214/etl-ald-log-2-2017121416")

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    //每个指标求停留时长时需要用到这个表
    spark.sql("select ak,uu,at,dr,ifo,hour from logs where ev='app'").createOrReplaceTempView("app_tmp")
    spark.sqlContext.cacheTable("app_tmp")
    //求出ev=app的ak，uu，每个指标算新用户时用到  ifo
    spark.sql("select ak,uu from app_tmp where ifo='true' group by ak,uu").createOrReplaceTempView("nu_app_daily")
    spark.sqlContext.cacheTable("nu_app_daily")
    spark.sql("select ak,uu,hour from app_tmp where ifo='true' group by ak,uu,hour").createOrReplaceTempView("nu_app_hourly")
    spark.sqlContext.cacheTable("nu_app_hourly")

    println("创建visitora_page...")
    logs.filter("ev='page' and ag_ald_link_key!='null' and ag_ald_position_id!='null' and ag_ald_media_id!='null' and scene!='null'and(scene!='1020'or scene!='1089'  or scene!='1001' or scene!='1023' or scene!='1069' or scene!='1024' or scene!='1006') ")
      .createOrReplaceTempView("visitora_page")

    spark.sql(
      """
        |select trace.link_key ag_ald_link_key,trace.app_key ak,trace.media_id ag_ald_media_id,vp.v,
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

    val linkDailyCollect: LinkAnalysis = new LinkDailyCollect(spark)
    val linkDailyDetails: LinkAnalysis = new LinkDailyDetails(spark)
    val mediaDailyDetails: LinkAnalysis = new MediaDailyDetails(spark)
    val positionDailyDeails: LinkAnalysis = new PositionDailyDeails(spark)
    val linkHourly: LinkAnalysis = new LinkHourlyDetails(spark)
    val mediaHourly: LinkAnalysis = new MediaHourlyDetails(spark)
    val positionHourly: LinkAnalysis = new PositionHourlyDetails(spark)

    val objAd = List[LinkAnalysis](linkDailyCollect,linkDailyDetails, mediaDailyDetails, positionDailyDeails, linkHourly,
      mediaHourly, positionHourly)
    objAd.foreach(x => {
      //每个指标调用执行
      adDailyData(x)
    })

    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("nu_app_daily")
    spark.sqlContext.uncacheTable("nu_app_hourly")
    spark.sqlContext.uncacheTable("visitora_page")
  }

  def adDailyData(linkAnalysis: LinkAnalysis): Unit = {
    linkAnalysis.clickCount()
    linkAnalysis.newUser()
    linkAnalysis.visitorCount()
    linkAnalysis.atSession2()
    linkAnalysis.authuserCount()
    linkAnalysis.openCount()
    linkAnalysis.atSession()
    linkAnalysis.totalPageCount()
    linkAnalysis.totalStayTime()
    linkAnalysis.secondaryAvgStayTime()
    linkAnalysis.visitPageOnce()
    linkAnalysis.bounceRate()
    linkAnalysis.insert2db()
  }

  //补7天或30天数据
  def fillSevenOrThirtyData(spark: SparkSession, numDays: String): Unit = {
    println(s"开始处理$numDays 天的数据")
    //val newSession=spark.newSession()
    new SingleAdLinkCollect().executeSingleSevenOrThirty(spark, numDays)
    new SevenAndThirtyDaysCollect().executeSevenOrDaysCollect(spark, numDays)

  }
}
