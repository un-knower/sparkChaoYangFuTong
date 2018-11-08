package aldwxstat.aldwls

import java.util.Properties

import aldwxconfig.ConfigurationUtil
import aldwxutils.ArgsTool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/13.
  */

object AldSceneDetails {

//  //  需要在group by 后面分组的（正常情况下就是不同的维度）
//  val dimensionAnalysis = "brand"

  /**
    * 主函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AldSceneDetails")
//      .master("local")
      //.config("spark.sql.shuffle.partitions", 300)
      .getOrCreate()

    //    连接数据库
//    val url = DBConf.url
//    val prop = new Properties()
//    prop.put("driver", DBConf.driver)
//    prop.setProperty("user", DBConf.user)
//    prop.setProperty("password", DBConf.password)
//    spark.read.jdbc(url, "ald_cms_scene", prop).createOrReplaceTempView("ald_cms_scene")
    val url = ConfigurationUtil.getProperty("jdbc.url")
    val prop = new Properties()
    prop.put("driver", ConfigurationUtil.getProperty("jdbc.driver"))
    prop.setProperty("user", ConfigurationUtil.getProperty("jdbc.user"))
    prop.setProperty("password", ConfigurationUtil.getProperty("jdbc.pwd"))
    spark.read.jdbc(url, "ald_cms_scene", prop).createOrReplaceTempView("ald_cms_scene")
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
      fillDailyData(args,spark)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      fillDailyData(args,spark)
    }
  }


  //真正执行昨天或某一天数据
  def fillDailyData(args: Array[String],spark: SparkSession): Unit = {
    val logs = ArgsTool.getLogs(args,spark,ConfigurationUtil.getProperty("tongji.parquet"))
    //val logs = ArgsTool.getLogs(args,spark,ConfigurationUtil.getProperty("tencent.parquet"))
    //val logs = ArgsTool.getTencentLogs(args,spark,ConfigurationUtil.getProperty("tencent.parquet"))

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    spark.sql(s"SELECT a.ak,a.uu,a.dr,a.ifo,a.ev,a.at,a.pp,a.st,a.hour,a.scene scene,b.scene_group_id scene_group FROM logs  a left join ald_cms_scene b on a.scene = b.sid where hour !='null'")
      .createOrReplaceTempView("scene_data_all")
    //数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
    //    获取需要在ev=app中计算的指标的字段
    spark.sql("select ak,uu,at,scene,scene_group,hour,ifo,dr from scene_data_all where ev='app'")
      .createOrReplaceTempView("app_tmp")
//    spark.sql("select * from app_tmp").show()
    spark.sqlContext.cacheTable("app_tmp")
    //    获取需要在ev=page中计算的指标的字段
    spark.sql("select ak,uu,scene,scene_group,hour,pp,at from scene_data_all where ev='page'")
      .createOrReplaceTempView("visitor_page")

    spark.sqlContext.cacheTable("visitor_page")

    //    创建对象，将sparksession和需要执行的维度出进去
    val sceneDailyDetails: PublicModularDetails = new PublicModularDetails(spark, "scene")
    val sceneGroupDailyDetails: PublicModularDetails = new PublicModularDetails(spark, "scene_group")
    val hourSceneDailyDetails: PublicHourModularDetails = new PublicHourModularDetails(spark, "scene")
    val hourSceneGroupDailyDetails: PublicHourModularDetails = new PublicHourModularDetails(spark, "scene_group")
    //    调用最终执行的接口函数
    adDailyData(sceneDailyDetails)
    new PublicMysqlInsertDetails(spark).sceneInsert2db()

    adDailyData(sceneGroupDailyDetails)
    new PublicMysqlInsertDetails(spark).sceneGroupInsert2db()
    hourAdDailyData(hourSceneDailyDetails)
    new PublicMysqlInsertDetails(spark).hourSceneInsert2db()
    hourAdDailyData(hourSceneGroupDailyDetails)
    new PublicMysqlInsertDetails(spark).hourSceneGroupInsert2db()
    //    清除缓存表
    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("visitor_page")
  }

  /**
    * 对创建的类进行实现
    *
    * @param sceneAnalysis
    */
  def adDailyData(sceneAnalysis: PublicModularDetails): Unit = {
    sceneAnalysis.newUser()
    sceneAnalysis.visitorCount()
    sceneAnalysis.openCount()
    sceneAnalysis.totalPageCount()
    sceneAnalysis.totalStayTime()
    sceneAnalysis.secondaryAvgStayTime()
    sceneAnalysis.visitPageOnce()
    sceneAnalysis.bounceRate()
  }

  /**
    * 对创建的类进行实现（小时）
    *
    * @param hourAnalysis
    */
  def hourAdDailyData(hourAnalysis: PublicHourModularDetails): Unit = {
    hourAnalysis.newUser()
    hourAnalysis.visitorCount()
    hourAnalysis.openCount()
    hourAnalysis.totalPageCount()
    hourAnalysis.totalStayTime()
    hourAnalysis.secondaryAvgStayTime()
    hourAnalysis.visitPageOnce()
    hourAnalysis.bounceRate()
  }
}
