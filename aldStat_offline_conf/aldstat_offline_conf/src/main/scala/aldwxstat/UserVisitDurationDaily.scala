package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2017/8/8.
  * 忠诚度访问时长
  */
object UserVisitDurationDaily {
  // 设置日志级别为WARN
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val yesterday = TimeUtil.processArgs(args)
    //获取昨天时间
    val ss = SparkSession.builder().appName(this.getClass.getName)
      .getOrCreate()
    //读取数据

    val datafream = ArgsTool.getLogs(args, ss, ConfigurationUtil.getProperty("tongji.parquet"))
    //创建临时视图
    datafream.createTempView("t_user_loyal")
    //这是为了补0根据访问时长的维度来创建
    val df = ss.createDataFrame(List(("1", 0), ("2", 0), ("3", 0), ("4", 0), ("5", 0), ("6", 0), ("7", 0), ("8", 0)))
    val lpDF = df.withColumnRenamed("_1", "page_depth").withColumnRenamed("_2", "ak") //指定字段
    lpDF.createTempView("df_a")
    ss.sql("select distinct ak  from t_user_loyal").createTempView("df_b")
    //取出所有ak
    //两表进行cross join
    val view = ss.sql("select df_b.ak app_key, df_a.page_depth from df_a CROSS JOIN df_b  ").createTempView("df_c")

    //不同访问时长人数
    //计算 用户访问时长 用最大值减去最小值
    ss.sql("SELECT ak,uu,at,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) p_dr " +
      "FROM t_user_loyal WHERE ev='page' GROUP BY ak,uu,at").createTempView("a")
    val t = ss.sql("SELECT ak app_key,uu,at, (case when (p_dr >=0 and p_dr<=2)  then '1' " +
      "when (p_dr >=3 and p_dr<=5)   then '2' " +
      "when (p_dr >=6 and p_dr<=10)  then '3' " +
      "when (p_dr >=11 and p_dr<=20)  then '4' " +
      "when (p_dr >=21 and p_dr<=30)  then '5' " +
      "when (p_dr >=31 and p_dr<=50)  then '6' " +
      "when (p_dr >=51 and p_dr<=100)  then '7' " +
      "else '8' end) dr_pepth FROM a  GROUP BY ak,uu,at,p_dr").createTempView("b")
    //ak uu分组查询打开次数
    ss.sql("SELECT ak app_key,uu,at,count(DISTINCT at) as pps   FROM" +
      " t_user_loyal WHERE ev='app' GROUP BY ak,uu,at").createTempView("c")
    //连接访问时长标记表表
    ss.sql("SELECT b.app_key,b.uu,b.dr_pepth,c.pps FROM b LEFT JOIN c ON b.app_key = c.app_key AND" +
      " b.uu = c.uu AND b.at=c.at ").createTempView("d")
    //访问人数 打开次数
    ss.sql("SELECT app_key,dr_pepth,count(distinct uu) as num_of_user, " +
      "  sum(pps) as vists_times " +
      " from d group by app_key,dr_pepth  ").createTempView("e")
    //打开总人数 打开页面总次数
    ss.sql("select app_key, sum (num_of_user) as users,sum(vists_times) as times from e group by app_key ").createTempView("f")

    //
    val t_f = ss.sql("SELECT e.app_key,e.dr_pepth,e.num_of_user, " +
      " (cast(num_of_user as double) / cast(f.users as double)) AS percent ," +
      " e.vists_times, " +
      " (cast(vists_times as double) / cast(f.times as double)) AS t_percent " +
      " from e left join f on e.app_key=f.app_key ").na.fill(0)
    t_f.createTempView("ff")
    val result = ss.sql("select df_c.app_key," + yesterday + " day,df_c.page_depth,ff.num_of_user,ff.percent,ff.vists_times" +
      " ,ff.t_percent, NOW() as update_at from df_c  left join ff on (df_c.page_depth=ff.dr_pepth and df_c.app_key=ff.app_key) where df_c.app_key!='null'  ").na.fill(0)

    try {
      result.foreachPartition((rows: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = s"insert into aldstat_visit_duration (app_key,day,visit_duration,visitor_count, visitor_ratio," +
          s" open_count,open_ratio,update_at)" +
          s"values (?,?,?,?,?,?,?,?) ON " +
          s"DUPLICATE KEY UPDATE visitor_count=?, visitor_ratio=?, open_count=?,open_ratio=?"
        rows.foreach(r => {
          val app_key = r.get(0)
          val day = r.get(1)
          val visit_duration = r.get(2)
          val visitor_count = r.get(3)
          val visitor_ratio = r.get(4)
          val open_count = r.get(5)
          val open_ratio = r.get(6)
          val update_at = r.get(7)
          params.+=(Array[Any](app_key, day, visit_duration, visitor_count, visitor_ratio, open_count, open_ratio, update_at, visitor_count, visitor_ratio, open_count, open_ratio))
        })
        JdbcUtil.doBatch(sqlText, params)
      })
    } catch {
      case e: Exception => e.printStackTrace()
      //        connection.close()
    }
    //    关闭资源
    ss.close()

  }
}
