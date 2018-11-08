package aldwxstat.aldwls

import aldwxconfig.ConfigurationUtil
import aldwxutils.ArgsTool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/13.modified by clark 2018-08-02
  */

object AldTrendDetails {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AldTrendDetails")
      .getOrCreate()
    //    将参数传入和sparksession传入，真正执行
    execute(args, spark)
    spark.close()
  }

  /**
    * 判断执行昨日数据还是某一天的数据
    *
    * @param args
    * @param spark
    */
  def execute(args: Array[String], spark: SparkSession): Unit = {

    ArgsTool.analysisArgs(args)
    if (args.length == 0) {
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      fillDailyData(args, spark)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      fillDailyData(args, spark)
    }
  }


  //真正执行昨天或某一天数据
  def fillDailyData(args: Array[String], spark: SparkSession): Unit = {
    val logs = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))
    logs.createOrReplaceTempView("logs")
    //数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
    //    获取需要在ev=app中计算的指标的字段
    spark.sql("select ak,uu,at,ifo,dr,hour,ev from logs").createOrReplaceTempView("app_view")
    spark.sqlContext.cacheTable("app_view")
    //    获取需要在ev=page中计算的指标的字段
    spark.sql("select ak,uu,pp,at,hour,dr from logs where ev='page'")
      .createOrReplaceTempView("page_view")

    spark.sql("select ak,path,ct,tp,hour from logs where ev='event'")
      .createOrReplaceTempView("visitor_event")

    spark.sqlContext.cacheTable("page_view")

    //    创建对象，将sparksession和需要执行的维度出进去
    val trendDailyDetails: TrendModularDetails = new TrendModularDetails(spark, "trend")
    val hourDailyDetails: TrendModularDetails = new TrendModularDetails(spark, "hour")
    //    调用最终执行的接口函数
    adDailyData(trendDailyDetails)
    new PublicMysqlInsertDetails(spark).trendInsert2db()
    adDailyData(hourDailyDetails)
    new PublicMysqlInsertDetails(spark).hourTrendInsert2db()
    //    清除缓存表
    spark.sqlContext.uncacheTable("app_view")
    spark.sqlContext.uncacheTable("page_view")
  }

  /**
    * 对创建的类进行实现
    *
    * @param trendAnalysis
    */
  def adDailyData(trendAnalysis: TrendModularDetails): Unit = {
    trendAnalysis.newUser()
    trendAnalysis.visitorCount()
    trendAnalysis.openCount()
    trendAnalysis.totalPageCount()
    trendAnalysis.totalStayTime()
    trendAnalysis.secondaryAvgStayTime()
    trendAnalysis.visitPageOnce()
    trendAnalysis.bounceRate()
    trendAnalysis.shareCount()
  }
}
