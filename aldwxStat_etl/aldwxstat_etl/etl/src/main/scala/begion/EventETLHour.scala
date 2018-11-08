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

    //==========================================================1
    /*
    *gcs:
    *设定log日志的级别
    */
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 12)
      .appName(this.getClass.getName)
      .getOrCreate()

    //==========================================================2
    /*
    *gcs:
    *创建一个时间工具类
    */
    val a = new TimeUtil
    //获取今天时间
    val today =  a.processArgs(args)

    //获取当前小时的前一个小时
    val hour: String = a.processArgsHour(args)

   val  ti= today+hour //gcs:ti的格式是2018062509  前8为代表年月日，后两位代表小时

    //==========================================================3
    /*
    *gcs:
    *定义一个配置项。将File的hdfs的url地址
    */
    val conf = new Configuration()
    conf.set("fs.defaultFS",  FieldName.hdfsurl)

    //==========================================================4
    /*
    *gcs:
    *创建一个HDFS的fileSystem对象。
    */
    val fileSystem = FileSystem.get(conf)


    //==========================================================5
    /*
    *gcs:
    *提取出parquet的路径
    */
    //要读取的日志目录
    val paths: String = FieldName.parquet
    val fs = fileSystem.listStatus(new Path(paths + today)) //gcs:显示fileSystem的状态fs
    val listPath = FileUtil.stat2Paths(fs) //gcs:将fs的状态转换为文件的路径paths

    for (d <- listPath) { //gcs:便利这个path路径
      if (d.toString.contains(s"$ti")){ //gcs:如果path当中包含 $ti，即包含，2018062509 这个日期参数和时间参数的话
        println(d)
        //==========================================================6
        /*
        *gcs:
        *此时将 $d 的路径中的文件读取出来，并且把ev="event"的类型数据读取出来
        */
        val df = spark.read.parquet(s"$d").filter(col("ev")==="event").repartition(100)

        //==========================================================7
        /*
        *gcs:
        *将 d 按照"/" 参数将参数区分开
        */
        val pathnames =d.toString.split("/")
        val pathname = pathnames(pathnames.length-1) //gcs:分开的最后一个参数是文件的名字
        val tablenames = pathname.split("-") //gcs:将pathname按照-这个参数分隔开
        val t = tablenames(tablenames.length-1) //gcs: t这个参数是什么鬼
        val t2 = tablenames(tablenames.length-2 ) //gcs:t2这个参数又是什么意思
        val tablename = t+t2  //gcs:把t和t2拼接起来
        //==========================================================8
        /*
        *gcs:
        *将dataSet当中的每一行记录都是用map方法将parquet转换为json。
        * 之后把(ak,tp,st,ss)这些字段提取出出来
        */
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

        //==========================================================9
        /*
        *gcs:
        *设定一个表头，dataFrame的表头
        */
        //指定dataframe的类型
        val schema = StructType(
          List(
            StructField("ak", StringType, true),
            StructField("tp", StringType, true),
            StructField("st", StringType, true),
            StructField("ss", StringType, true)
          )
        )

        //==========================================================10
        /*
        *gcs:
        *创造一个dataFrame对象
        */
        //初始化dataframe
        val df_1 = spark.createDataFrame(rdd,schema)

        //==========================================================11
        /*
        *gcs:
        *创建一个视图
        */
        df_1.createTempView(s"tmp$tablename")

        //==========================================================12
        /*
        *gcs:
        *将ak+tp使用MD5加密，生成一个event_key
        */
        //udf  函数  (将 tp 和 ak 加密)
        spark.udf.register("udf",((s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))))
        def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("")


        //==========================================================13
        /*
        *gcs:
        *注册一个MySql的自定义的函数，函数的名字为time2date
        */
        spark.udf.register("time2date",(s: String)=>{
          val regex = """([0-9]+)""".r   //gcs:注册一个模式匹配。这样regex就成为了Regex的类型的了

          //将时间切换成 11 位的int 类型
          val res = s.substring(0,s.length-3) match{ //gcs:将s从(0,length-3)提取出来
            case regex(num)=>num   //gcs:如果regex是一个num类型的数据，就会返回num值。
            case _ => "0" //gcs:否则就会返回0
          }


          //gcs:将String类型的数据转换成为Int类型的数据
          val a = Integer.parseInt(res)
          val d:Long=a.toLong*1000;  //gcs:我觉得这里应该的 a 是一个分钟级别的数。a*1000，是把分钟转换成秒级别的数
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //gcs:创建一个用于指定时间格式的类型
          sdf.format(d) //gcs:将这个时间d转换成指定的格式
        })


        //==========================================================14
        /*
        *gcs:
        *udf是创建的用户进行MD5加密的函数 笔记12
        * time2data是用来调整时间的格式的 笔记13
        */
        val rs = spark.sql(s"select ak,tp,udf(ss),time2date(st) from tmp$tablename").distinct()

        //==========================================================15
        /*
        *gcs:
        *将ak,tp,ss,st字段提取出来之后，存储进入ald_event当中
        */
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

              //==========================================================16
              /*
              *gcs:
              *这条语句显示，只有parquet当中收取到了新的app_key、event_key下的这个事件的名字，此时重新更新这个事件。否则的话是不会更新这个事件的
              * 那么有一个问题来了，是什么因素，导致那个周日的ak的用户的ald_event事件的状态被设置为了0.
              *  Field          | Type         | Null | Key | Default           | Extra          |
+----------------+--------------+------+-----+-------------------+----------------+
| id             | int(11)      | NO   | PRI | NULL              | auto_increment |
| app_key        | varchar(255) | NO   | MUL | NULL              |                |
| ev_id          | varchar(255) | NO   |     | NULL              |                |
| event_key      | varchar(255) | NO   |     | NULL              |                |
| ev_name        | varchar(255) | NO   |     | NULL              |                |
| ev_status      | tinyint(1)   | NO   |     | 1                 |                |
| ev_update_time | timestamp    | NO   |     | CURRENT_TIMESTAMP |                |
+----------------+--------------+------+-----+-------------------+----------------+

                可以看到ald_event这个表的事件的有效性默认是被设置为1的，到底在哪个过程，会产生改变用户状态的，使得用户的ald_event事件的状态从1变成了0
              */
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
