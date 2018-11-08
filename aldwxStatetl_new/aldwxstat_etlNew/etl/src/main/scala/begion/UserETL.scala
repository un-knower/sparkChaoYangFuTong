package begion

import java.sql.{Connection, DriverManager}

import jdbc.MySqlPool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count}
import aldwxutil.{DBConf, TimeUtil}

/**
  * Created by zhangyanpeng on 2017/8/1.
  *
  * 关于用户的ETL   将数据保存到 mysql
  */
object UserETL {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 12)
      .appName(this.getClass.getName)
      .getOrCreate()
    //创建对象
    val a = new TimeUtil
    val processArgs = a.processArgs2(args)
    println(processArgs)
    //清洗的路径  入库源
    //val path="hdfs://10.56.0.36:9000/ald_log_etl/"+processArgs  //(测试环境)
    val path="hdfs://10.0.100.17:4007/ald_log_etl/"+processArgs    //生产
    //val path="C:\\Users\\zhangyanpeng\\Desktop\\数据\\20171030\\*1.json"


    //读取到的源数据
    val df: DataFrame = spark.read.json(path).repartition(100)

    //.filter("hour != null and hour!='null' ")   filter(col("ev")==="app")
    val df_1 =df.filter("ev='app' and hour!='null'")
      .select(
        df("ak"),
        df("uu"),
        df("province"),
        df("scene"),
        df("city"),
        df("nt"),
        df("qr"),
        df("lang"),
        df("wv"),
        df("wsdk"),
        df("hour"),
        df("day"),
        df("ifo")
      ).groupBy("ak","uu","province","scene","city","nt","qr","lang","wv","wsdk","day","hour","ifo")
      .agg(
        count("uu") as "open_count" //日志的相同的数据的个数
      ).distinct()



    var conn:Connection = null
    df_1.foreachPartition((rows:Iterator[Row])=>{
      conn =MySqlPool.getJdbcConn()
      val statement = conn.createStatement
      conn.setAutoCommit(false)
      try {
        conn.setAutoCommit(false)
        rows.foreach(r => {
          val ak = r(0)
          val uu = r(1)
          val province = r(2)
          val scene = r(3)
          val city = r(4)
          val nt = r(5)
          val qr = r(6)
          val lang = r(7)
          val wv = r(8)
          val wsdk = r(9)
          val day = r(10)
          val hour = r(11)
          val ifo = r(12)
          val open_count = r(13)

          val sql = s"insert into ald_user_etl (ak,uu,scene,province,city,nt,qr,lang,wv,wsdk,hour,day,ifo,open_count) values ('${ak}', '${uu}', '${scene}','${province}', '${city}', '${nt}', '${qr}', '${lang}', '${wv}', '${wsdk}', '${hour}', '${day}', '${ifo}','${open_count}') ON DUPLICATE KEY UPDATE open_count='${open_count}'"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()
      }catch {
        case e: Exception => e.printStackTrace()
          conn.close()
      }
    })

    spark.stop()

  }
}
