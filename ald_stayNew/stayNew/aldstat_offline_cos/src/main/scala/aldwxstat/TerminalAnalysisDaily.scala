package aldwxstat

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by 18510 on 2017/8/22. 终端分析
  */
object TerminalAnalysisDaily {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkseesion = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    //获取昨天时间yyyyMMdd
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val yesterday = TimeUtil.processArgs(args)
    val dateTime = TimeUtil.getTimestamp(yesterday)

    val terminal_sum = Array("wvv", "nt", "lang", "wv", "sv", "wsdk", "ww_wh")
    //    val paths = aldwxutils.DBConf.hdfsUrl + "/"
    val paths = ConfigurationUtil.getProperty("hdfsUrl") + "/"
    //    val df = Aldstat_args_tool.analyze_args(args,sparkseesion,DBConf.hdfsUrl)
    val df = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))

    for (terminal <- terminal_sum) {
      df.createOrReplaceTempView(s"phone_brand_$terminal")
      var tmp_args = ""

      if (terminal == "ww_wh") {
        //新访客(new_comer_count)
        val newnum = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT uu) new_comer_count FROM phone_brand_${terminal} WHERE ev = 'app' AND ifo='true' GROUP BY ak,concat(ww,'*',wh)")
        println("----------------------开始-----------------------")
        //        newnum.show()
        //访问人数(visitor_count)
        val visnum = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT uu) visitor_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,concat(ww,'*',wh)")
        //打开次数(open_count)
        val opennum = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT at) open_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,concat(ww,'*',wh)")
        //        opennum.show()

        //页面总访问量
        val totalpage = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(pp) total_page_count FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,concat(ww,'*',wh)")
        //        totalpage.show()
        //总停留时长
        //        concat(ww,'*',wh)
        val totaltimes = sparkseesion.sql(s"SELECT ak app_key,eeee.aaa tmp_sum,sum(x) total_stay_time FROM (SELECT ak,concat(ww,'*',wh) aaa,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,concat(ww,'*',wh)) eeee group by ak,eeee.aaa")
        //        totaltimes.show()
        //      sparkseesion.sql(s"SELECT ak,at,${tmp_args},cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,${tmp_args}").show()
        //创建临时表
        newnum.createTempView(s"newnum1_$terminal")
        visnum.createTempView(s"visnum1_$terminal")
        opennum.createTempView(s"opennum1_$terminal")
        totalpage.createTempView(s"totalpage1_$terminal")
        totaltimes.createTempView(s"totaltimes1_$terminal")

        //次均
        val pl = sparkseesion.sql(s"SELECT a.app_key app_key,a.tmp_sum tmp_sum,cast(b.total_stay_time/a.visitor_count as int) avg_stay_time FROM visnum1_$terminal a left join totaltimes1_$terminal b on a.app_key=b.app_key and a.tmp_sum = b.tmp_sum")
        //        pl.show()
        pl.createTempView(s"pl1_$terminal")

        //访问一次页面的次数
        val times_one2 = sparkseesion.sql(s"select tmp.ak app_key,tmp.tmp_sum_one tmp_sum,sum(tmp.cp) one_cp from (SELECT ak,at ,concat(ww,'*',wh) tmp_sum_one,COUNT(pp) cp FROM phone_brand_$terminal where ev = 'page' GROUP BY ak,at,pp,concat(ww,'*',wh)) tmp group by tmp.ak,tmp.at,tmp.tmp_sum_one")
        //        times_one2.show()
        times_one2.createTempView(s"times_one2_$terminal")
        val times_one3 = sparkseesion.sql(s"select app_key,tmp_sum,sum(one_cp) one_page_count from times_one2_$terminal where one_cp = 1 group by app_key,tmp_sum")
        times_one3.createTempView(s"times_one_$terminal")
        //页面跳出率
        val bounce = sparkseesion.sql(s"select a.app_key app_key,a.tmp_sum tmp_sum,cast(b.one_page_count/a.total_page_count as float) bounce_rate from totalpage1_$terminal a left join times_one_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum")
        bounce.createTempView(s"bounce1_$terminal")
        val result = sparkseesion.sql(s"SELECT a.app_key,a.tmp_sum type_value,a.total_page_count,b.visitor_count,c.open_count,d.total_stay_time,e.avg_stay_time,f.new_comer_count,g.one_page_count,h.bounce_rate from totalpage1_$terminal a left join visnum1_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum left join opennum1_$terminal c on b.app_key = c.app_key and b.tmp_sum = c.tmp_sum left join totaltimes1_$terminal d on c.app_key = d.app_key and c.tmp_sum = d.tmp_sum left join pl1_$terminal e on d.app_key = e.app_key and d.tmp_sum = e.tmp_sum left join newnum1_$terminal f on e.app_key = f.app_key and e.tmp_sum = f.tmp_sum left join times_one_$terminal g on g.app_key = f.app_key and g.tmp_sum = f.tmp_sum left join bounce1_$terminal h on g.app_key = h.app_key and g.tmp_sum = h.tmp_sum").distinct().na.fill(0).withColumn("type", lit(terminal))
        // result.show()
        result.foreachPartition((rows: Iterator[Row]) => {

          val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
          val sqlText = s"insert into aldstat_terminal_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,type,type_value)" +
            s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
            s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?,type_value=?"

          rows.foreach(r => {
            val day = dateTime
            val update_at = UpDataTime
            val app_key = r.get(0)
            val type_values = r.get(1)
            val total_page_count = r.get(2)
            val visitor_count = r.get(3)
            val open_count = r.get(4)
            val total_stay_time = r.get(5)
            val avg_stay_time = r.get(6)
            val new_comer_count = r.get(7)
            val one_page_count = r.get(8)
            val bounce_rate = r.get(9)
            var ty = ""
            if (r.get(10).toString == "" || r.get(10).toString == "null" || r.get(10) == null || r.get(10).toString == "undefined") {
              ty = "未知"
            } else {
              ty = r.get(10).toString()
            }

            params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, ty, type_values))
          })
          JdbcUtil.doBatch(sqlText,params)
        })
      } else {
        tmp_args = terminal
        //新访客(new_comer_count)
        val newnum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT uu) new_comer_count FROM phone_brand_${terminal} WHERE ev = 'app' AND ifo='true' GROUP BY ak,${tmp_args} ")
        //访问人数(visitor_count)
        val visnum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT uu) visitor_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,${tmp_args} ")
        //打开次数(open_count)
        val opennum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT at) open_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,${tmp_args}")
        //页面总访问量
        val totalpage = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(pp) total_page_count FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,${tmp_args}")
        //    totalpage.show()
        //总停留时长
        val totaltimes = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,sum(x) total_stay_time FROM (SELECT ak,${tmp_args},cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,${tmp_args}) group by ak,${tmp_args}")
        //      sparkseesion.sql(s"SELECT ak,at,${tmp_args},cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,${tmp_args}").show()
        //创建临时表
        newnum.createTempView(s"newnum1_$terminal")
        visnum.createTempView(s"visnum1_$terminal")
        opennum.createTempView(s"opennum1_$terminal")
        totalpage.createTempView(s"totalpage1_$terminal")
        totaltimes.createTempView(s"totaltimes1_$terminal")
        //        newnum.show()
        //        visnum.show()
        //        opennum.show()
        //        totalpage.show()
        //        totaltimes.show()
        //次均
        val pl = sparkseesion.sql(s"SELECT a.app_key app_key,a.tmp_sum tmp_sum,cast(b.total_stay_time/a.visitor_count as int) avg_stay_time FROM visnum1_$terminal a left join totaltimes1_$terminal b on a.app_key=b.app_key and a.tmp_sum = b.tmp_sum")
        pl.createTempView(s"pl1_$terminal")

        //访问一次页面的次数
        val times_one2 = sparkseesion.sql(s"select tmp.ak app_key,tmp.tmp_sum_one tmp_sum,sum(tmp.cp) one_cp from (SELECT ak,at ,${tmp_args} tmp_sum_one,COUNT(pp) cp FROM phone_brand_$terminal where ev = 'page' GROUP BY ak,at,pp,${tmp_args}) tmp group by tmp.ak,tmp.at,tmp.tmp_sum_one")

        times_one2.createTempView(s"times_one2_$terminal")
        val times_one3 = sparkseesion.sql(s"select app_key,tmp_sum,sum(one_cp) one_page_count from times_one2_$terminal where one_cp = 1 group by app_key,tmp_sum")
        times_one3.createTempView(s"times_one_$terminal")
        //页面跳出率
        val bounce = sparkseesion.sql(s"select a.app_key app_key,a.tmp_sum tmp_sum,cast(b.one_page_count/a.total_page_count as float) bounce_rate from totalpage1_$terminal a left join times_one_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum")
        //    pl.show()
        //    times_one3.show()
        //    bounce.show()
        bounce.createTempView(s"bounce1_$terminal")
        //    bounce.show()
        //    val result = sparkseesion.sql("SELECT " + yesterday + s" day,${UpDataTime} update_at,a.app_key,a.new_comer_count,b.visitor_count,c.open_count,d.total_page_count,e.total_stay_time,f.avg_stay_time,k.bounce_rate,a.tmp_sum type_value FROM newnum1_$tmp_args a left join visnum1_$tmp_args b on a.app_key = b.app_key and a.tmp_sum =b.tmp_sum left join opennum1_$tmp_args c on b.app_key = c.app_key and b.tmp_sum =c.tmp_sum left join totalpage1_$tmp_args d on c.app_key = d.app_key and c.tmp_sum = d.tmp_sum left join totaltimes1_$tmp_args e on d.app_key = e.app_key and d.tmp_sum = e.tmp_sum left join pl1_$tmp_args f on e.app_key = f.app_key and e.tmp_sum = f.tmp_sum left join bounce1_$tmp_args k on f.app_key=k.ak and f.tmp_sum = k.tmp_sum").withColumn("type",lit(tmp_args)).distinct().na.fill(0)
        val result = sparkseesion.sql(s"SELECT a.app_key,a.tmp_sum type_value,a.total_page_count,b.visitor_count,c.open_count,d.total_stay_time,e.avg_stay_time,f.new_comer_count,g.one_page_count,h.bounce_rate from totalpage1_$terminal a left join visnum1_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum left join opennum1_$terminal c on b.app_key = c.app_key and b.tmp_sum = c.tmp_sum left join totaltimes1_$terminal d on c.app_key = d.app_key and c.tmp_sum = d.tmp_sum left join pl1_$terminal e on d.app_key = e.app_key and d.tmp_sum = e.tmp_sum left join newnum1_$terminal f on e.app_key = f.app_key and e.tmp_sum = f.tmp_sum left join times_one_$terminal g on g.app_key = f.app_key and g.tmp_sum = f.tmp_sum left join bounce1_$terminal h on g.app_key = h.app_key and g.tmp_sum = h.tmp_sum").distinct().na.fill(0).withColumn("type", lit(terminal))

        //result.show()
        result.foreachPartition((rows: Iterator[Row]) => {

          val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
          val sqlText = s"insert into aldstat_terminal_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,type,type_value)" +
            s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
            s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?,type_value=?"

          rows.foreach(r => {
            val day = dateTime
            val update_at = UpDataTime
            val app_key = r.get(0)
            val type_values = r.get(1)
            val total_page_count = r.get(2)
            val visitor_count = r.get(3)
            val open_count = r.get(4)
            val total_stay_time = r.get(5)
            val avg_stay_time = r.get(6)
            val new_comer_count = r.get(7)
            val one_page_count = r.get(8)
            val bounce_rate = r.get(9)
            val ty = r.get(10)

            params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, ty, type_values, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, type_values))
          })
          JdbcUtil.doBatch(sqlText,params)
        })

      }
    }
    sparkseesion.close()
  }
}
