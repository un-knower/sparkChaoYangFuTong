package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * update by youle on 2018/8/2
  */
object HierarchyShare {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")
    val sparkseesion = SparkSession
      .builder()
      //      .master("local")
      //      .config("spark.sql.shuffle.partitions", 200)
      .appName(this.getClass.getName)
      //.config("spark.sql.small.file.combine", "true")
      .getOrCreate()

    val yesterday = aldwxutils.TimeUtil.processArgs(args)

    val df_tmp = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))
    val rdd = df_tmp.toJSON.rdd.map(line => {
      val jsonLine = JSON.parseObject(line)
      val uu = jsonLine.get("uu")
      val ak = jsonLine.get("ak")
      val src = jsonLine.get("wsr_query_ald_share_src")
      //初始化值，判断src是否为空，若不为空则去最后一个值
      var src1 = ""
      var src2 = ""
      if (src != null) {
        if(src.toString.split("\\,").length ==1){
          src1=src.toString
        }
        if(src.toString.split("\\,").length ==2){
          src2=src.toString.split("\\,")(1)
        }
      }
      if(src==null) src1=null
      val tp = jsonLine.get("tp")
      val ct = jsonLine.get("ct")
      val ifo = jsonLine.get("ifo")
      val ev = jsonLine.get("ev")
      val at = jsonLine.get("at")
      val scene=jsonLine.get("scene")
      Row(uu,  ak, src1, src2,tp, ct, ifo, ev,  at,scene)
    })
    val schema = StructType(List(
      StructField("uu", StringType, true),
      StructField("ak", StringType, true),
      StructField("src1", StringType, true),
      StructField("src2", StringType, true),
      StructField("tp", StringType, true),
      StructField("ct", StringType, true),
      StructField("ifo", StringType, true),
      StructField("ev", StringType, true),
      StructField("at", StringType, true),
      StructField("scene", StringType, true)
    )
    )
    val df = sparkseesion.createDataFrame(rdd, schema)
    //创建临时表
    df.createTempView("share")
    sparkseesion.sqlContext.cacheTable("share")


    /**
      * 一度层级分享
      */

    //一度分享次数和人数
    val share_first = sparkseesion.sql("SELECT ak ,COUNT(distinct uu) share_user_count,COUNT(at) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and src1='null' GROUP BY ak")
    share_first.createTempView("share_first")

    //一度分享带来新增
    val share_new_first = sparkseesion.sql("SELECT ak,COUNT(DISTINCT uu) new_count FROM share WHERE ev='event'  AND tp='ald_share_click' and (src1='null' or src1!='') and (scene='1007' or scene='1008' or scene='1044' or scene='1036') and uu in (select uu from share where ev='app' and ifo='true') GROUP BY ak")
    share_new_first.createTempView("share_new_first")

    //一度分享打开数（回流量）
    val share_open_first = sparkseesion.sql("SELECT ak ,COUNT(at) share_open_count FROM share WHERE ev='event' and tp='ald_share_click' and (scene='1007' or scene='1008' or scene='1044' or scene='1036') and (src1='null' or src1!='') GROUP BY ak")
    share_open_first.createTempView("share_open_first")

    //一度分享回流比
    val share_reflux_ratio_first = sparkseesion.sql("SELECT a.ak,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from share_first a left join share_open_first b on a.ak = b.ak")
    share_reflux_ratio_first.createTempView("share_reflux_ratio_first")

    //一度分享次数和新增
    val new_and_count_first = sparkseesion.sql("select a.ak,a.share_user_count,a.share_count,b.new_count from share_first a left join share_new_first b on a.ak = b.ak")
    new_and_count_first.createTempView("new_and_count_first")

    //一度分享打开人数次数join回流量
    val open_and_user_first = sparkseesion.sql("select a.ak,b.share_open_count,a.share_reflux_ratio from share_reflux_ratio_first a left join share_open_first b on a.ak = b.ak")
    open_and_user_first.createTempView("open_and_user_first")

    //一度汇总
    val result_first = sparkseesion.sql("select a.ak," + yesterday + " day,b.share_user_count first_share_user_count,b.new_count first_share_new_user_count,b.share_count first_share_count,a.share_open_count first_backflow,a.share_reflux_ratio first_backflow_ratio from open_and_user_first a left join new_and_count_first b on a.ak = b.ak").na.fill(0)
    result_first.createTempView("result_first")

    /**
      * 二度层级分享
      */

    //二度分享次数和人数
    val share_second = sparkseesion.sql("SELECT ak ,COUNT(distinct uu) share_user_count,COUNT(at) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and src1!='' and src1!='null' GROUP BY ak")
    share_second.createTempView("share_second")

    //二度分享带来新增
    val share_new_second = sparkseesion.sql("SELECT ak,COUNT(DISTINCT uu) new_count FROM share WHERE ev='event'  AND tp='ald_share_click' and src2!='' and (scene='1007' or scene='1008' or scene='1044' or scene='1036') and uu in (select uu from share where ev='app' and ifo='true' and src2!='') GROUP BY ak")
    share_new_second.createTempView("share_new_second")

    //二度分享打开数（回流量）
    val share_open_second = sparkseesion.sql("SELECT ak ,COUNT(at) share_open_count FROM share WHERE ev='event' and (scene='1007' or scene='1008' or scene='1044' or scene='1036') and tp='ald_share_click' and src2!='' GROUP BY ak")
    share_open_second.createTempView("share_open_second")

    //二度分享回流比
    val share_reflux_ratio_second = sparkseesion.sql("SELECT a.ak,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from share_second a left join share_open_second b on a.ak = b.ak")
    share_reflux_ratio_second.createTempView("share_reflux_ratio_second")

    //二度分享次数和新增
    val new_and_count_second = sparkseesion.sql("select a.ak,a.share_user_count,a.share_count,b.new_count from share_second a left join share_new_second b on a.ak = b.ak")
    new_and_count_second.createTempView("new_and_count_second")

    //二度分享打开人数次数join回流量
    val open_and_user_second = sparkseesion.sql("select a.ak,b.share_open_count,a.share_reflux_ratio from share_reflux_ratio_second a left join share_open_second b on a.ak = b.ak")
    open_and_user_second.createTempView("open_and_user_second")

    //二度汇总
    val result_second = sparkseesion.sql("select a.ak,b.share_user_count second_share_user_count,b.new_count second_share_new_user_count,b.share_count second_share_count,a.share_open_count second_backflow,a.share_reflux_ratio second_backflow_ratio from open_and_user_second a left join new_and_count_second b on a.ak = b.ak").na.fill(0)
    result_second.createTempView("result_second")

    //结果汇总
    val result = sparkseesion.sql("select a.ak," + yesterday + " day,a.first_share_user_count, a.first_share_count, a.first_share_new_user_count, a.first_backflow, a.first_backflow_ratio,b.second_share_user_count, b.second_share_count, b.second_share_new_user_count, b.second_backflow, b.second_backflow_ratio, unix_timestamp() as update_at from result_first a left join result_second b on a.ak=b.ak").na.fill(0)
    result.createTempView("result")

    //入库
    result.foreachPartition(line => {
      dataframe2mysqlShare(line)
    })

    sparkseesion.close()
  }

  def dataframe2mysqlShare(iterator: Iterator[Row]): Unit = {

    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into ald_hierarchy_share(app_key, day, first_share_user_count, first_share_count, first_share_new_user_count, frist_backflow, frist_backflow_ratio, secondary_share_user_count, secondary_share_count, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio,update_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?)" +
      s" ON DUPLICATE KEY UPDATE first_share_user_count=?, first_share_count=?, first_share_new_user_count=?, frist_backflow=?, frist_backflow_ratio=?, secondary_share_user_count=?, secondary_share_count=?, secondary_share_new_user_count=?, secondary_backflow=?, secondary_backflow_ratio=?,update_at=?"
    iterator.foreach(r => {
      val app_key = r(0)
      val day = r(1)
      val first_share_user_count = r(2)
      val first_share_count = r(3)
      val first_share_new_user_count = r(4)
      val first_backflow = r(5)
      val first_backflow_ratio = r(6)
      val second_share_user_count = r(7)
      val second_share_count = r(8)
      val second_share_new_user_count = r(9)
      val second_backflow = r(10)
      val second_backflow_ratio = r(11)
      val update_at=r(12)

      params.+=(Array[Any](app_key, day, first_share_user_count, first_share_count, first_share_new_user_count, first_backflow, first_backflow_ratio,
        second_share_user_count, second_share_count, second_share_new_user_count, second_backflow, second_backflow_ratio,update_at,first_share_user_count, first_share_count, first_share_new_user_count, first_backflow, first_backflow_ratio,
        second_share_user_count, second_share_count, second_share_new_user_count, second_backflow, second_backflow_ratio,update_at))
    })
    JdbcUtil.doBatch(sqlText, params)
  }


}

