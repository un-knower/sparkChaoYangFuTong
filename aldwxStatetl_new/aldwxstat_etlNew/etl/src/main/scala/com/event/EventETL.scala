package com.event

import java.security.MessageDigest
import java.sql.Connection
import java.text.SimpleDateFormat

import aldwxutil.TimeUtil
import com.alibaba.fastjson.JSON
import jdbc.MySqlPool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import property.FieldName


/**
  * Created by gaoxiang on 2018/1/11.
  */
object EventETL {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
//      .config("spark.speculation", true)
//      .config("spark.sql.caseSensitive", true)
      .getOrCreate()
    val aldTimeUtil = new TimeUtil
    //获取今天时间
    val today = aldTimeUtil.processArgs(args)
    //获取当前小时的前一个小时
    val hour: String = aldTimeUtil.processArgsHour(args)
    jsonEtlHour(spark, aldTimeUtil, today, hour)

    spark.stop()
  }

  def jsonEtlHour(spark: SparkSession, aldTimeUtil: TimeUtil, today: String, hour: String): Unit = {
//    val conf = new Configuration()
//    conf.set("fs.defaultFS", FieldName.hdfsurl)
//    val fileSystem = FileSystem.get(conf)
//    //要读取的日志目录
//    val paths: String = FieldName.jsonpath
//    val fs = fileSystem.listStatus(new Path(paths + today))
//    val listPath = FileUtil.stat2Paths(fs)
//    for (readPath <- listPath) {
//      if (readPath.toString.contains(s"$hour.json")) {
//        val getSavePath = readPath.toString.split("/")
//        val savePath = getSavePath(5)
//        val savePathNames = savePath.split("\\.")
//        val savePathName = savePathNames(0) + "-" + savePathNames(1)
//        try {
//          val DS: DataFrame = spark.read.text(s"$readPath")
//
////          new etlHourJson(DS, aldTimeUtil, today, savePathName) //清洗统计的json
////          new errorEtlHourJson(DS, aldTimeUtil, today, savePathName) //清洗错误日志的json
//        } catch {
//          case e: Exception => e.printStackTrace()
//          case _: Exception => println("出错路径---" + readPath.toString)
//        }
//
//      }
//
//    }
//    new parquetHourEtl(spark, aldTimeUtil, today, hour) //清洗统计的parquet
//    new errorParquetHourEtl(spark, aldTimeUtil, today, hour) //清洗错误的parquet
    new eventETLHour(spark, aldTimeUtil, today, hour) //事件的清洗

  }


}
/**
  * 事件etl清洗
  *
  * @param spark
  * @param aldTimeUtil
  * @param today
  * @param hour
  */
class eventETLHour(spark: SparkSession, aldTimeUtil: TimeUtil, today: String, hour: String) extends Serializable {

  //判断是否包含某个小时的文件 需要带的字符串
  val isHour = today + hour
  @transient
  val conf = new Configuration()
  conf.set("fs.defaultFS", FieldName.hdfsurl)
  @transient
  val fileSystem = FileSystem.get(conf)
  //要读取的日志目录
  @transient
  val paths: String = FieldName.parquetpath
  @transient
  val fs = fileSystem.listStatus(new Path(paths + today))
  //当前某个小时的所有文件夹
  @transient
  val listPath = FileUtil.stat2Paths(fs)
  for (readPath <- listPath) {
    //通过for循环处理读判断是否包含要处理小时的文件夹
    if (readPath.toString.contains(s"$isHour")) {
      println(readPath)
      val df = spark.read.option("mergeSchema", "true").parquet(s"$readPath").filter(col("ev") === "event")
      //切分出一个表名要求每次循环里面的名字都不是不一样的
      val pathnames = readPath.toString.split("/")
      val pathname = pathnames(pathnames.length - 1)
      val tablenames = pathname.split("-")
      val tableOne = tablenames(tablenames.length - 1)
      val tableTwo = tablenames(tablenames.length - 2)
      val tablename = tableOne + tableTwo
      val rdd = df.toJSON.rdd.map(
        line => {
          val jsonLine = JSON.parseObject(line)
          val ak: String = jsonLine.get("ak").toString
          val tp: String = jsonLine.get("tp").toString
          var st = jsonLine.get("st")
          if (st == null) {
            st = ""
          } else { // st 字段为空则让 st = ""
            st = st.toString //  否则就直接转字符串
          }
          val ss = ak + tp
          Row(ak, tp,st, ss)
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
      val oneDF: DataFrame = spark.createDataFrame(rdd, schema)
      //初始化dataframe

      oneDF.createTempView(s"tmp$tablename")
      //udf  函数  (将 tp 和 ak 加密)
      spark.udf.register("udf", ((s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))))

      def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("")


      spark.udf.register("time2date", (s: String) => {
        val regex = """([0-9]+)""".r
        //将时间 切成 11 位的int 类型
        var res = ""
        try {
          res = s.substring(0, s.length - 3) match {
            case regex(num) => num
            case _ => "0"
          }
        } catch {
          case e: StringIndexOutOfBoundsException =>
            println(s"$s ：这条数据长度不够")
        }

        val stime = Integer.parseInt(res)
        val stimeLong: Long = stime.toLong * 1000;
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        sdf.format(stimeLong)
      })

      val result = spark.sql(s"select ak,tp,udf(ss) from tmp$tablename").distinct()
     val update=aldTimeUtil.nowTime()
      //入库到mysql 表中
      var conn: Connection = null
      result.foreachPartition((rows: Iterator[Row]) => {
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
            val ev_time = update
            //如果数据库已经存在ev_id 这里只更新时间
            val sql =
              s"""
                 |insert into ald_event (app_key,ev_id,event_key,ev_name,ev_update_time) values
                 |("${ak}","${ev_id}","${event_key}","${ev_name}","${ev_time}") ON DUPLICATE KEY UPDATE ev_update_time="${ev_time}"
              """.stripMargin
            statement.addBatch(sql)
          })
          statement.executeBatch
          conn.commit()
        } catch {
          case e: Exception => e.printStackTrace()
            conn.close()
        }
      })
    }
  }
}









