package begion

import java.security.MessageDigest
import java.sql.Connection
import java.text.SimpleDateFormat

import aldwxutil.TimeUtil
import com.alibaba.fastjson.JSON
import jdbc.MySqlPool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import property.FieldName

/**
  * Created by gaoxiang on 2017/12/15.
  */
object EventETLHour {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 12)
      .appName(this.getClass.getName)
      .getOrCreate()

    val a = new TimeUtil
    //获取今天时间
    val today =  a.processArgs(args)

    //获取当前小时的前一个小时
    val hour: String = a.processArgsHour(args)

   val  ti= today+hour

    val conf = new Configuration()
    conf.set("fs.defaultFS",  FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    //要读取的日志目录
    val paths: String = FieldName.parquet
    val fs = fileSystem.listStatus(new Path(paths + today))
    val listPath = FileUtil.stat2Paths(fs)
    for (d <- listPath) {
      if (d.toString.contains(s"$ti")){
        println(d)
        val df = spark.read.parquet(s"$d").filter(col("ev")==="event").repartition(100)
        val pathnames =d.toString.split("/")
        val pathname = pathnames(pathnames.length-1)
        val tablenames = pathname.split("-")
        val t = tablenames(tablenames.length-1)
        val t2 = tablenames(tablenames.length-2 )
        val tablename = t+t2
        val rdd = df.toJSON.rdd.map(
          line=>{
            val jsonLine = JSON.parseObject(line)
            val ak:String=jsonLine.get("ak").toString
            val tp:String=jsonLine.get("tp").toString
            val st=jsonLine.get("st").toString
            val ss =ak+tp
            Row(ak,tp,st,ss)
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
        val df_1 = spark.createDataFrame(rdd,schema)
        df_1.createTempView(s"tmp$tablename")

        //udf  函数  (将 tp 和 ak 加密)
        spark.udf.register("udf",((s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))))
        def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("")


        spark.udf.register("time2date",(s: String)=>{
          val regex = """([0-9]+)""".r
          //将时间 切成 11 位的int 类型
          val res = s.substring(0,s.length-3) match{
            case regex(num)=>num
            case _ => "0"
          }
          val a = Integer.parseInt(res)
          val d:Long=a.toLong*1000;
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          sdf.format(d)
        })


        val rs = spark.sql(s"select ak,tp,udf(ss),time2date(st) from tmp$tablename").distinct()

        //假如到mysql 表中
        var conn:Connection = null
        rs.foreachPartition((rows:Iterator[Row])=>{
          val conn =MySqlPool.getJdbcConn()
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
              val ev_time= r(3)

              val sql = s"insert into ald_event (app_key,ev_id,event_key,ev_name,ev_update_time) values ('${ak}','${ev_id}','${event_key}','${ev_name}','${ev_time}') ON DUPLICATE KEY UPDATE ev_update_time='${ev_time}'"
              statement.addBatch(sql)
            })
            statement.executeBatch
            conn.commit()
          }catch {
            case e: Exception => e.printStackTrace()
              conn.close()
          }
        })
      }
    }
    spark.stop()
  }
}
