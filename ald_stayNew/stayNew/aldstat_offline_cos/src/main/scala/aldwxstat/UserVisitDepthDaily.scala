package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2017/8/21.
  * 访问深度
  */
object UserVisitDepthDaily {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //获取昨天时间
    val yesterday = TimeUtil.processArgs(args)
    val ss = SparkSession.builder().appName(this.getClass.getName)
      .getOrCreate()
    val datafream = ArgsTool.getLogs(args, ss, ConfigurationUtil.getProperty("tongji.parquet"))
    //创建临时视图
    datafream.createTempView("t_user_loyal")
    //给某些ak下某些维度没有的补0
    val df = ss.createDataFrame(List(("1", 0), ("2", 0), ("3", 0), ("4", 0), ("5", 0), ("6", 0)))
    val zeroDF = df.withColumnRenamed("_1", "page_depth").withColumnRenamed("_2", "ak") //指定字段
    zeroDF.createTempView("df_a") //创建视图
    ss.sql("select distinct ak  from t_user_loyal").createTempView("df_b") //去重相同的ak
    //通过corss join 两个表
    val view = ss.sql("select df_b.ak app_key, df_a.page_depth from df_a CROSS JOIN df_b   ").createTempView("df_c")
    //计算不同的访问深度
    ss.sql("SELECT ak app_key,uu,at, (case when count(pp)=1  then '1' " +
      "when count(pp)=2  then '2' " +
      "when count(pp)=3 then '3' " +
      "when count(pp)=4 then '4' " +
      "when count(pp)>=5 and count(pp)<=10 then '5' " +
      "else '6' end) page_depth FROM t_user_loyal where ev ='page' GROUP BY ak,uu,at")
      .createTempView("b")
    //计算打开次数
    ss.sql("select ak app_key ,uu,at,count(DISTINCT at) as pps from t_user_loyal where ev ='app' group by ak,uu,at")
      .createTempView("a")
    //打开次数与标记表连接
    ss.sql("select b.app_key ,b.uu,b.page_depth ,a.pps from b left join a on b.app_key=a.app_key and b.uu=a.uu and b.at=a.at")
      .na.fill(0)
      .createTempView("c")
    //计算不同维度的访问人数和打开次数
    ss.sql("SELECT app_key,page_depth,count(distinct uu) as num_of_user,sum(pps) as vists_times  from c group by app_key,page_depth  ")
      .createTempView("d")
    //计算所有访问人数 和打开次数
    ss.sql("select app_key, sum (num_of_user) as users,sum (vists_times) as times from d group by app_key ")
      .createTempView("e")
    val t_f = ss.sql(s"SELECT d.app_key,  page_depth, num_of_user, " +
      " (cast(num_of_user as double) / cast(e.users as double)) AS percent ," +
      " vists_times, " +
      " (cast(vists_times as double) / cast(e.times as double)) AS t_percent " +
      "  from d left join e on d.app_key=e.app_key group by d.app_key,d.page_depth,d.vists_times,d.num_of_user,e.users,e.times  ").na.fill(0)
    t_f.createTempView("f")

    val result = ss.sql("select df_c.app_key," + yesterday + " day,df_c.page_depth,f.num_of_user,f.percent,f.vists_times" +
      " ,f.t_percent, NOW() as update_at from df_c  left join f on (df_c.page_depth=f.page_depth and df_c.app_key=f.app_key) ").na.fill(0)
    println("打印计算结果")
    result.show()

    try {
      result.foreachPartition((rows: Iterator[Row]) => {
        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = "insert into aldstat_visit_depth (app_key,day,visit_depth,visitor_count, visitor_ratio," +
          "open_count,open_ratio,update_at)" +
          "values (?,?,?,?,?,?,?,?) ON " +
          "DUPLICATE KEY UPDATE visitor_count=?, visitor_ratio=?, open_count=?,open_ratio=?"

        rows.foreach(r => {
          val app_key = r.get(0)
          val day = r.get(1)
          val visit_depth = r.get(2)
          val visitor_count = r.get(3)
          val visitor_ratio = r.get(4)
          val open_count = r.get(5)
          val open_ratio = r.get(6)
          val update_at = r.get(7)

          params.+=(Array[Any](app_key, day, visit_depth, visitor_count, visitor_ratio, open_count, open_ratio, update_at,visitor_count,visitor_ratio,open_count,open_ratio))
        })
        JdbcUtil.doBatch(sqlText, params)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
    ss.close()
  }
}
