package begion

import java.security.MessageDigest
import java.sql.Connection
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import jdbc.MySqlPool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import aldwxutil.{DBConf, TimeUtil}

/**
  * Created by zhangyanpeng on 2017/8/28.
  */
object EventETL {
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
    val paquter = s"hdfs://10.0.100.17:4007/ald_log_parquet/${processArgs}/*/part-*" //(测试环境)
    val path = "hdfs://10.0.100.17:4007/ald_log_etl/" + processArgs //生产

    //val df = spark.read.json("C:\\Users\\zhangyanpeng\\Desktop\\log1").filter(col("ev")==="event")
    val df = spark.read.parquet(paquter).filter(col("ev") === "event").repartition(100)

    val rdd = df.toJSON.rdd.map(
      line => {
        val jsonLine = JSON.parseObject(line)
        val ak: String = jsonLine.get("ak").toString
        val tp: String = jsonLine.get("tp").toString
        val st = jsonLine.get("st").toString
        val ss = ak + tp
        Row(ak, tp, st, ss)
      }
    ).distinct()

    //指定dataframe的类型
    val schema = StructType(
      List(
        StructField("ak", StringType, true),
        StructField("tp", StringType, true),
        StructField("st", StringType, true),
        StructField("ss", StringType, true)
      )
    )

    //初始化dataframe
    val df_1 = spark.createDataFrame(rdd, schema)
    df_1.createTempView("tmp")

    //udf  函数  (将 tp 和 ak 加密)
    spark.udf.register("udf", ((s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))))

    def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("")


    spark.udf.register("time2date", (s: String) => {
      val regex = """([0-9]+)""".r
      //将时间 切成 11 位的int 类型
      val res = s.substring(0, s.length - 3) match {
        case regex(num) => num
        case _ => "0"
      }
      val a = Integer.parseInt(res)
      val d: Long = a.toLong * 1000;
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.format(d)
    })


    val rs = spark.sql("select ak,tp,udf(ss),time2date(st) from tmp").distinct()

    //假如到mysql 表中
    var conn: Connection = null
    rs.foreachPartition((rows: Iterator[Row]) => {
      val conn = MySqlPool.getJdbcConn()
      conn.setAutoCommit(false)
      val statement = conn.createStatement
      try {
        conn.setAutoCommit(false)
        rows.foreach(r => {
          val ak = r(0)
          val ev_id = r(1)
          val event_key = r(2)
          var ev_name = ""
          r(1) match {
            case "ald_reachbottom" => ev_name = "页面触底(阿拉丁默认)"
            case "ald_pulldownrefresh" => ev_name = "下拉刷新(阿拉丁默认)"
            case _ => ev_name = r(1).toString
          }

          val ev_time = r(3)

          val sql = s"insert into ald_event (app_key,ev_id,event_key,ev_name,ev_update_time) values ('${ak}','${ev_id}','${event_key}','${ev_name}','${ev_time}') ON DUPLICATE KEY UPDATE ev_update_time='${ev_time}'"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()
      } catch {
        case e: Exception => e.printStackTrace()
          conn.close()
      }
    })

    spark.stop()
  }
}
