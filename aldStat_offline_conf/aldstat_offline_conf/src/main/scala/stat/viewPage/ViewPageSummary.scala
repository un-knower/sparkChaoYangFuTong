package stat.viewPage

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by admin on 2018/5/8.
  * 受访页
  * 汇总统计
  */
object ViewPageSummary {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      //.master("local[2]")
      .appName(this.getClass.getName).getOrCreate()

    //获取原始数据，过滤，创建临时表并缓存
    ArgsTool.analysisArgs(args)
    val logs_df = ArgsTool.getDailyDataFrame(spark, ConfigurationUtil.getProperty("tongji.parquet"))
    logs_df.filter("ev='page' and ak!=''").createTempView("logs_page")
    spark.sqlContext.cacheTable("logs_page")
    logs_df.filter("ev!='event' and ak!=''").createOrReplaceTempView("logs_app")
    spark.sqlContext.cacheTable("logs_app")
    logs_df.filter("ev='event' and ak!=''").createOrReplaceTempView("logs_event")
    spark.sqlContext.cacheTable("logs_event")

    statData(spark)

    batchInsert(spark,args)

    tearDown(spark)

    spark.stop()
  }

  /**
    * 统计各指标数据
    * @param spark
    */
  def statData(spark:SparkSession) = {
    /**
      * 统计所有的退出页信息
      * 退出页含义：在一个会话中，访问的最后一个页面
      * 计算规则：根据会话（at）分组，根据时间（st）排序，取出每个分组中的最后一条数据，就是退出页*/
    spark.sql(
      """
        |SELECT * FROM
        |(SELECT ROW_NUMBER()over(partition by at ORDER BY et DESC) rowId, ak, uu, at, pp from logs_page) as demo
        |WHERE rowId=1 """.stripMargin).createOrReplaceTempView("exit_table")
    spark.sqlContext.cacheTable("exit_table")

    /**
      * 访问人数
      * 算法：count（uu） ,需要去重
      */
    spark.sql(
      """
        |SELECT ak,COUNT(DISTINCT uu) AS visitor_count FROM logs_page GROUP BY ak
      """.stripMargin).createOrReplaceTempView("visitor_count_table")
    spark.sqlContext.cacheTable("visitor_count_table")

    /**
      * 访问次数 - 退出页次数
      * 算法：count（pp）
      * 注：作为退出页被访问的次数
      */
    spark.sql(
      """
        |SELECT ak, COUNT(pp) AS visit_times FROM exit_table GROUP BY ak
      """.stripMargin).createOrReplaceTempView("visit_times_table")
    spark.sqlContext.cacheTable("visit_times_table")

    /**
      * 页面总访问次数
      * 算法：count(pp)
      * 全量数据，统计这些页面总共被访问的次数
      */
    spark.sql(
      """
        |SELECT ak, COUNT(pp) AS visit_times FROM logs_page GROUP BY ak
      """.stripMargin).createOrReplaceTempView("total_visit_times_table")
    spark.sqlContext.cacheTable("total_visit_times_table")

   /**
      * 打开次数
      * 算法:count(at),需要去重
      * 数据源：logs_page
      */
    spark.sql(
      """
        |SELECT ak, COUNT(DISTINCT at) AS open_times FROM logs_page GROUP BY ak
      """.stripMargin).createOrReplaceTempView("open_times_table")
    spark.sqlContext.cacheTable("open_times_table")

    /**
      * 退出率
      * 算法：退出页总数 除以 总访问次数
      */
    spark.sql(
      """
        |SELECT tvtt.ak AS ak,CAST(vtt.visit_times / tvtt.visit_times  as float) AS exit_prob
        |FROM total_visit_times_table AS tvtt ,visit_times_table vtt
        |WHERE tvtt.ak = vtt.ak
      """.stripMargin).createOrReplaceTempView("exit_prob_table")
    spark.sqlContext.cacheTable("exit_prob_table")

    /**
      * 求每个会话的时长信息
      * 算法:求每个会话中dr的最大值
      * 日志级别：app
      */
    spark.sql(
      """
        |SELECT ak ,at, MAX(dr) AS at_duration
        |FROM logs_app GROUP BY ak ,at
      """.stripMargin).createOrReplaceTempView("at_duration_table")
    spark.sqlContext.cacheTable("at_duration_table")

    /**
      * 总时长
      */
    spark.sql(
      """
        |SELECT ak, SUM(at_duration) AS total_duration
        |FROM at_duration_table GROUP BY ak
      """.stripMargin).createOrReplaceTempView("total_duration_table")
    spark.sqlContext.cacheTable("total_duration_table")

    /**
      * 次均停留时长
      * 算法：用户的总停留时长/打开次数
      */
    spark.sql(
      """
        |SELECT tdt.ak AS ak,CAST(tdt.total_duration / ott.open_times / 1000 as float) AS avg_duration
        |FROM total_duration_table AS tdt ,open_times_table ott
        |WHERE tdt.ak = ott.ak
      """.stripMargin).createOrReplaceTempView("avg_duration_table")
    spark.sqlContext.cacheTable("avg_duration_table")

    /**
      * 分享次数
      */
    spark.sql(
      """
        |SELECT ak, COUNT(path) share_count
        |FROM logs_event
        |WHERE ct!= 'fail' and ct !='null' and tp='ald_share_status'
        |GROUP BY ak
      """.stripMargin).createOrReplaceTempView("share_count_table")
    spark.sqlContext.cacheTable("share_count_table")

  }

  /**
    * 整合数据，批量入库
    * @param spark
    */
  def batchInsert(spark:SparkSession,args: Array[String])={
    ArgsTool.analysisArgs(args)
    val day = ArgsTool.day
    val result = spark.sql(
      """
        |SELECT tvtt.ak AS app_key , tvtt.visit_times AS total_visit_times,
        | vct.visitor_count ,
        | vtt.visit_times AS visit_times ,
        | ept.exit_prob ,
        | tdt.total_duration ,
        | adt.avg_duration ,
        | sct.share_count ,
        | ott.open_times
        |FROM total_visit_times_table AS tvtt
        |LEFT JOIN visitor_count_table AS vct ON tvtt.ak = vct.ak
        |LEFT JOIN visit_times_table AS vtt ON tvtt.ak = vtt.ak
        |LEFT JOIN exit_prob_table AS ept ON tvtt.ak = ept.ak
        |LEFT JOIN total_duration_table AS tdt ON tvtt.ak = tdt.ak
        |LEFT JOIN avg_duration_table AS adt ON tvtt.ak = adt.ak
        |LEFT JOIN share_count_table AS sct ON tvtt.ak = sct.ak
        |LEFT JOIN open_times_table AS ott ON tvtt.ak = ott.ak
      """.stripMargin).na.fill(0)

    result.foreachPartition(rows =>{
      val params = new ArrayBuffer[Array[Any]]()
      val sql =
        """
          |insert into aldstat_daily_page_view
          |(app_key,day,page_count,abort_page_count,visitor_count,open_count,total_time,avg_stay_time,abort_ratio,update_at,share_count)
          |values (?,?,?,?,?,?,?,?,?,now(),?)
          |ON DUPLICATE KEY UPDATE
          |page_count=?, abort_page_count=?,visitor_count=?,open_count=?,total_time=?,avg_stay_time=?,abort_ratio=?,
          |update_at=now(),share_count=?
        """.stripMargin

      try {
        rows.foreach(r => {
          val app_key = r(0).toString
          val page_count = r(1).toString //页面总访问次数
          val abort_page_count = r(3).toString //退出页次数
          val visitor_count = r(2).toString //访问人数
          val open_count = r(8).toString //打开次数

          val total_time_msec = r.get(5).toString //总停留时长 毫秒级别
          var total_time = ""
          if(total_time_msec.length > 3){
            total_time = total_time_msec.substring(0,total_time_msec.length-3) //总停留时长 秒级别
            //限定总停留时长的值为11为，这样会导致总停留时长的不准确，前台不会用到这个字段
            if(total_time.length > 11){
              total_time = total_time.substring(0,11)
            }
          }else{
            total_time = total_time_msec
          }

          val avg_stay_time = r(6).toString //次均停留时间
          val abort_ratio = r(4).toString //退出率
          val share_count = r(7).toString//分享次数
          //val update_at = TimeUtil.nowInt()

          params.+=(Array[Any](app_key, day, page_count, abort_page_count,visitor_count,open_count,total_time,avg_stay_time,abort_ratio,share_count,
            page_count, abort_page_count,visitor_count,open_count,total_time,avg_stay_time,abort_ratio,share_count))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }

      JdbcUtil.doBatch(sql, params) //批量入库
    })
  }

  /**
    * 销毁资源
    * @param spark
    */
  def tearDown(spark:SparkSession)={
    spark.sqlContext.uncacheTable("exit_table")
    spark.sqlContext.uncacheTable("visitor_count_table")
    spark.sqlContext.uncacheTable("visit_times_table")
    spark.sqlContext.uncacheTable("total_visit_times_table")
    spark.sqlContext.uncacheTable("open_times_table")
    spark.sqlContext.uncacheTable("exit_prob_table")
    spark.sqlContext.uncacheTable("at_duration_table")
    spark.sqlContext.uncacheTable("total_duration_table")
    spark.sqlContext.uncacheTable("avg_duration_table")
    spark.sqlContext.uncacheTable("share_count_table")
  }

}
