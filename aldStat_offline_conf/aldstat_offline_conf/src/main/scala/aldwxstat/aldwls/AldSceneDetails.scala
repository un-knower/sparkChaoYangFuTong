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

  /**
    * 主函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.speculation", true)
      .config("spark.sql.shuffle.partitions", 500)
      .config("spark.sql.planner.skewJoin", "true")
      .appName("SceneDetails")
      .getOrCreate()

    //    连接数据库
    val url = ConfigurationUtil.getProperty("jdbc.url")
    val prop = new Properties()
    prop.put("driver", ConfigurationUtil.getProperty("jdbc.driver"))
    prop.setProperty("user", ConfigurationUtil.getProperty("jdbc.user"))
    prop.setProperty("password", ConfigurationUtil.getProperty("jdbc.pwd"))
    spark.read.jdbc(url, "ald_cms_scene", prop).createOrReplaceTempView("ald_cms_scene")

    fillDailyData(args, spark)
    spark.close()
  }

  def fillDailyData(args: Array[String], spark: SparkSession): Unit = {
    val logs = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    spark.sql(s"SELECT a.ak,a.uu,a.dr,a.ifo,a.ev,a.at,a.pp,a.hour,a.scene scene,b.scene_group_id scene_group FROM logs  a left join ald_cms_scene b on a.scene = b.sid ")
      .createOrReplaceTempView("scene_data_all")
    spark.sql("select ak,uu,at,scene,scene_group,hour,ifo,dr from scene_data_all where ev='app'")
      .repartition(500)
      .createOrReplaceTempView("app_view")
    spark.sqlContext.cacheTable("app_view")
    spark.sql("select ak,uu,scene,scene_group,hour,pp,at,dr from scene_data_all where ev='page'")
      .repartition(500)
      .createOrReplaceTempView("page_view")
    spark.sqlContext.cacheTable("page_view")

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
    spark.sqlContext.uncacheTable("app_view")
    spark.sqlContext.uncacheTable("page_view")
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

