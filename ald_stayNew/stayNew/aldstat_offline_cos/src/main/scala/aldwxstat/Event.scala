package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2018/1/11.
  */
object Event {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()
    //获取昨天时间
    val yesterday = aldwxutils.TimeUtil.processArgs(args)
    allEventStatistics(ss, yesterday, args)

    ss.close()
  }


  def allEventStatistics(spark: SparkSession, yesterday: String, args: Array[String]) = {
    ArgsTool.analysisArgs(args)
    val du = ArgsTool.du
    //    val df = Aldstat_args_tool.analyze_args(args, spark, DBConf.hdfsUrl).filter("ev='event' and tp!='null'")
//    val df = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev='event' and tp!='null'")

//    val df =ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet")).filter("ev='event' and tp!='null'")

    val df = ArgsTool.getTencentSevenOrThirty(spark,du).filter("ev='event' and tp!='null'")

    df.createTempView("daily_event")
    new allEventCount(spark, yesterday, df, du).eventCount()
  }
}

class allEventCount(spark: SparkSession, yesterday: String, df: Dataset[Row], du:String) extends Serializable {
  def eventCount(): Unit = {
    //读取mysql 中 的数据（ald_event）
    val event_df = JdbcUtil.readFromMysql(spark, "(select app_key,ev_id,event_key,ev_name from ald_event) as event_df")
    //事件人数和打开次数
    val dataFrame = spark.sql("select ak,tp,count(distinct(uu)) count_uu,count(at) count_at,cast(count(at)/count(distinct(uu)) as float) trigger_user_count,now() time from daily_event group by ak,tp").distinct()
    //把读取数据库的数据和读取日志的数据进行join 条件是日志的tp = 数据库的ev_id
    val result = dataFrame.join(event_df, dataFrame("ak") === event_df("app_key") &&
      dataFrame("tp") === event_df("ev_id"))
      .select(
        event_df("app_key"),
        event_df("ev_id"),
        event_df("event_key"),
        dataFrame("count_uu"),
        dataFrame("count_at"),
        dataFrame("time"),
        dataFrame("trigger_user_count")
      )
    new eventInsert2Mysql(result, yesterday, du).insertmysql()
  }

}

class eventInsert2Mysql(result: DataFrame, yesterday: String, du:String) extends Serializable {
  def insertmysql(): Unit = {
    result.foreachPartition((row: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      var sqlText = ""
      row.foreach(r => {
        //conn.setAutoCommit(false)
        val app_key = r.get(0)
        val day = yesterday
        val ev_id = r.get(1)
        val event_key = r.get(2)
        val trigger_user_count = r.get(3) //触发人数
        val trigger_count = r.get(4) //触发次数
        val update_at = r.get(5)
        val avg_trigger_count = r.get(6) //人均触发数
        if (du == "") {
          sqlText = "insert into aldstat_daily_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
          params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
        } else {
          sqlText = s"insert into aldstat_${du}days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
          params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
        }

        /* for (i <- 0 until args.length) {
          if ("-d".equals(args(i))) {
            sqlText = "insert into aldstat_daily_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
            params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
          } else if ("-du".equals(args(i))) {
            if (i + 1 < args.length && args(i + 1).equals("7")) {
              sqlText = "insert into aldstat_7days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
              params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
            } else if (i + 1 < args.length && args(i + 1).equals("30")) {
              sqlText = "insert into aldstat_30days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
              params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
            }
          }
        }*/
      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}
