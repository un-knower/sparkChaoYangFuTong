package stat.entrance

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by admin on 2018/5/8.
  * 入口页
  * 汇总统计
  */
object EntrancePageSummary_30day {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      //.master("local[2]")
      .appName(this.getClass.getName).getOrCreate()

    //获取原始数据，过滤，创建临时表并缓存
    val logs_df = ArgsTool.getTencentLogs(args, spark, ConfigurationUtil.getProperty("tencent.parquet"))
//    val logs_df = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))
    logs_df.filter("ev='page' and ak!=''").createTempView("logs_page")
    spark.sqlContext.cacheTable("logs_page")
    logs_df.filter("ev='app' and ak!=''").createOrReplaceTempView("logs_app")
    spark.sqlContext.cacheTable("logs_app")

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
      * 统计所有的入口页信息
      * 入口页含义：在一个会话中，访问的第一个页面
      * 计算规则：根据会话（at）分组，根据时间（st）排序，取出每个分组中的第一条数据，就是入口页*/
    spark.sql(
      """
        |SELECT * FROM
        |(SELECT ROW_NUMBER()over(partition by at ORDER BY st ASC) rowId, ak, uu, at, pp from logs_page) as demo
        |WHERE rowId=1 """.stripMargin).createOrReplaceTempView("entrance_table")
    spark.sqlContext.cacheTable("entrance_table")

    /**
      * 访问人数
      * 算法：count（uu） ,需要去重
      */
    spark.sql(
      """
        |SELECT ak,COUNT(DISTINCT uu) AS visitor_count FROM entrance_table GROUP BY ak
      """.stripMargin).createOrReplaceTempView("visitor_count_table")
    spark.sqlContext.cacheTable("visitor_count_table")

    /**
      * 访问次数
      * 算法：count（pp）
      * 注：作为入口页被访问次数
      */
    spark.sql(
      """
        |SELECT ak, COUNT(pp) AS visit_times FROM entrance_table GROUP BY ak
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
      * 跳出页数量
      * 跳出页定义：一个会话（at）中，只访问了一个页面
      */
    spark.sql(
      """
        |SELECT ak, COUNT(pp_count) jump_out_times
        |FROM (SELECT ak,at,COUNT(DISTINCT pp) AS pp_count FROM logs_page GROUP BY  ak , at)
        |WHERE pp_count = 1
        |GROUP BY ak
      """.stripMargin).createOrReplaceTempView("jump_out_table")
    spark.sqlContext.cacheTable("jump_out_table")

    /**
      * 跳出率
      * 算法: 跳出页个数/打开次数
      * 数据源：logs_page
      */
    spark.sql(
      """
        |SELECT ott.ak AS ak, jot.jump_out_times / ott.open_times AS jump_out_prob
        |FROM open_times_table AS ott, jump_out_table AS jot
        |WHERE ott.ak = jot.ak
      """.stripMargin).createOrReplaceTempView("jump_out_prob_table")
    spark.sqlContext.cacheTable("jump_out_prob_table")

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
        |SELECT vtt.ak AS app_key, vtt.visit_times, vct.visitor_count, tvtt.visit_times AS total_visit_times,
        |ott.open_times, jot.jump_out_times, jopt.jump_out_prob, tdt.total_duration,adt.avg_duration
        |FROM visit_times_table vtt
        |LEFT JOIN visitor_count_table vct  ON vtt.ak = vct.ak
        |LEFT JOIN total_visit_times_table tvtt ON vtt.ak = tvtt.ak
        |LEFT JOIN open_times_table ott ON vtt.ak = ott.ak
        |LEFT JOIN jump_out_table jot ON vtt.ak = jot.ak
        |LEFT JOIN jump_out_prob_table jopt ON vtt.ak = jopt.ak
        |LEFT JOIN total_duration_table tdt ON vtt.ak = tdt.ak
        |LEFT JOIN avg_duration_table adt ON vtt.ak = adt.ak
      """.stripMargin).na.fill(0)

    result.foreachPartition(rows =>{
      val params = new ArrayBuffer[Array[Any]]()
      val sql =
        """
          |INSERT INTO aldstat_30days_entrance_page
          |(app_key,day,entry_page_count,one_page_count,page_count,visitor_count,open_count,total_time,avg_stay_time,bounce_rate,update_at)
          |VALUES(?,?,?,?,?,?,?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE one_page_count=?, entry_page_count=?,visitor_count=?,page_count=?,total_time=?,avg_stay_time=?,bounce_rate=?,open_count=?,update_at=?
        """.stripMargin

      try {
        rows.foreach(r => {
          val app_key = r(0).toString
          val ifp_count = r.get(1).toString //入口页次数
          val visitor_count = r.get(2).toString //访问人数
          val page_count = r.get(3).toString //访问次数

          val total_time_msec = r.get(7).toString //总停留时长 毫秒级别
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

          val avg_stay_time = r.get(8).toString //次均停留时长
          val one_page_count = r.get(5).toString //跳出页个数
          val bounce_rate = r.get(6).toString //跳出率
          val count_at = r.get(4).toString //总打开次数
          val update_at = TimeUtil.nowInt()

          params.+=(Array[Any](app_key, day, ifp_count, one_page_count, page_count, visitor_count, count_at, total_time, avg_stay_time, bounce_rate, update_at,
            one_page_count, ifp_count, visitor_count, page_count, total_time, avg_stay_time, bounce_rate, count_at, update_at))
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
    spark.sqlContext.uncacheTable("entrance_table")
    spark.sqlContext.uncacheTable("visitor_count_table")
    spark.sqlContext.uncacheTable("visit_times_table")
    spark.sqlContext.uncacheTable("total_visit_times_table")
    spark.sqlContext.uncacheTable("open_times_table")
    spark.sqlContext.uncacheTable("jump_out_table")
    spark.sqlContext.uncacheTable("jump_out_prob_table")
    spark.sqlContext.uncacheTable("total_duration_table")
    spark.sqlContext.uncacheTable("avg_duration_table")
  }

}
