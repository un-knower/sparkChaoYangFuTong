package aldwxstat.aldwls

import aldwxconfig.ConfigurationUtil
import aldwxutils.ArgsTool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/13.
  */

object AldTrendDetails {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AldTrendDetails")
      //      .master("local")
      //.config("spark.sql.shuffle.partitions", 12)
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
//    val logs =ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet"))

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    //数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
    //    获取需要在ev=app中计算的指标的字段
    spark.sql("select ak,uu,at,ifo,dr,hour from logs where ev='app' and hour !='null'").createOrReplaceTempView("app_tmp")
    spark.sqlContext.cacheTable("app_tmp")
    //    获取需要在ev=page中计算的指标的字段
    spark.sql("select ak,uu,pp,at,hour from logs where ev='page' and hour !='null'")
      .createOrReplaceTempView("visitor_page")

    spark.sql("select ak,path,ct,tp,hour from logs where ev='event' and hour !='null'")
      .createOrReplaceTempView("visitor_event")

    spark.sqlContext.cacheTable("visitor_page")

    //    创建对象，将sparksession和需要执行的维度出进去
    val trendDailyDetails: TrendModularDetails = new TrendModularDetails(spark, "trend")
    val hourDailyDetails: TrendModularDetails = new TrendModularDetails(spark, "hour")
    //    调用最终执行的接口函数
    adDailyData(trendDailyDetails)
    new PublicMysqlInsertDetails(spark).trendInsert2db()
    adDailyData(hourDailyDetails)
    new PublicMysqlInsertDetails(spark).hourTrendInsert2db()
    //    清除缓存表
    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("visitor_page")
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
