package com

//==========================================================
/*
*gcs:
*这个程序是目前的
*/
import aldwxutil.{TimeUtil, regex_rule}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import property.FieldName
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.security.MessageDigest
import java.sql.Connection
import java.text.SimpleDateFormat

import jdbc.MySqlPool


/**
  * Created by gaoxiang on 2018/1/11.
  */
object ETLHourAll {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.speculation", true)
      .config("spark.sql.caseSensitive", true)
      .getOrCreate()


    val aldTimeUtil = new TimeUtil
    //获取今天时间
    val today = aldTimeUtil.processArgs(args)
    //获取当前小时的前一个小时
    val hour: String = aldTimeUtil.processArgsHour(args) //gcs:获取-h后面的时间的参数

    //==========================================================0
    /*
    *gcs:
    *这个函数是非常的重要的，是程序分析de open count
    */
    jsonEtlHour(spark, aldTimeUtil, today, hour)

    spark.stop()
  }

  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-7-9 <br>
  * <b>description:</b><br>
    *   开始进行etl阶段的数据的分析
  * <b>param:</b><br>
    *@param spark 用来进行数据处理的sparkSession对象
    *@param aldTimeUtil 这个类是一个对象。是我们创建的专门为时间进行操作的TimeUtil对象。因为我们的TimeUtil对象不是创建为object，所以还需要创建一个这样的对象
    *@param today 这是一个时间。这个today是计算”今天的时间“
    *@param hour 获取当前小时的前一个小时
  * <b>return:</b><br>
    *   无
  */
  def jsonEtlHour(spark: SparkSession, aldTimeUtil: TimeUtil, today: String, hour: String): Unit = {

    //==========================================================2
    /*
    *gcs:
    *读取hdfs当中的文件的路径。创建fileSystem对象。
    * 并且获得文件系统的存储路径
    */
    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    //要读取的日志目录
    val paths: String = FieldName.jsonpath

    //==========================================================2
    /*
    *gcs:
    *paths+today => hdfs://10.0.100.17:4007/ald_jsonlogs/today  这个路径下存储着我们统计组今天收到的用户的所有的数据。我们每一天产生的数据都存储在 hdfs://10.0.100.17:4007/ald_jsonlogs/today 路径下面
    */
    val fs = fileSystem.listStatus(new Path(paths + today))  //gcs:today 是20180701 即在程序运行的时候的-d 2018-07-11 参数.
    val listPath = FileUtil.stat2Paths(fs)

    /*
    *gcs:
    *上面的两行代码子啊执行之后会将paths +today目录当中的所有的子目录都读取进了listPath当中，即listPath当中是00~23h 的所有的etl数据的文件夹.
    * 之后在 笔记2-1 当中就是各个小时的数据了
    */


    val json_paths = new collection.mutable.ArrayBuffer[String]()

    var savePathName = ""

    //==========================================================2-1
    /*
    *gcs:
    *
    */
    for (readPath <- listPath) {

      if (readPath.toString.contains(s"$today$hour")) {
        json_paths.append(readPath.toString)  //gcs:将每一个文件的路径存放到json_paths的路径当中
      }

    }

    //==========================================================3
    /*
    *gcs:
    *将json_paths当中的所有的目录下面的文件都读取出来，之后将读出来的文件都存储到DF当中
    */
    val DS: DataFrame = spark.read.text(json_paths: _*)

    savePathName = s"$today$hour" //gcs:设定文件的存储路径
    //==========================================================4
    /*
    *gcs:
    *
    */
    etlHourJson(DS, aldTimeUtil, today, savePathName) //清洗统计的json
    errorEtlHourJson(DS, aldTimeUtil, today, savePathName) //清洗错误日志的json
    parquetHourEtl(spark, aldTimeUtil, today, hour) //清洗统计的parquet
    errorParquetHourEtl(spark, aldTimeUtil, today, hour) //清洗错误的parquet

    //事件单独处理 eventETL
    //    new eventETLHour(spark, aldTimeUtil, today, hour) //事件的清洗


  }

  /**<br>gcs:<br>
    * 这个功能模块专门用户错误分析的数据分析使用。这是对从Flume当中读取出来的数据进行清洗的操作 <br>
    * 做了一次的清洗之后的操作之后，还是会把这个数据先作为json存储起来。之后再将json数据读取出来转为parquet文件 <br>
    *
    * @param DS 从Flume当中出来的最原始的数据。
    * @param aldTimeUtil 这个aldTimeUtil是什么意思
    * @param savePathName 文件的保存路径
    * */
  def errorEtlHourJson(DS: DataFrame, aldTimeUtil: TimeUtil, today: String, savePathName: String) {

    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    //==========================================================20
    /*
    *gcs:
    *读取error的当中的数据进行查找
    * fileSystem 是将这个json文件清洗完成之后，存储到的问题
    */
    if (fileSystem.exists(new Path(FieldName.errpath + s"/$today/etl-$savePathName"))) {
      fileSystem.delete(new Path(FieldName.errpath + s"/$today/etl-$savePathName"), true)
    }
    DS.toJSON.rdd.map(line => {
      try {
        if (line != null) {
          if (line.contains("ald_error_message") || line.contains("js_err")) {

            val rule = new regex_rule
            val js1 = JSON.parseObject(line)
            val jsline = js1.get("value").toString
            val js = JSON.parseObject(jsline)
            if (js.containsKey("ak")) {
              val ak = js.get("ak").toString
              if (rule.chenkak(ak)) {
                var jsonetl = new JSONObject()
                //获得所对应的时间戳
                if (js.containsKey("ts") && js.get("ts") != null && !js.get("ts").equals("")) {
                  val stvale = js.get("ts").toString
                  if (stvale.length == 13) {
                    val sss = stvale.toLong
                    jsonetl.put("st", sss)
                  }
                } else if (js.containsKey("st") && js.get("st") != null && !js.get("st").equals("")) {
                  val stvale = js.get("st").toString
                  if (stvale.length == 13) {
                    val sss = stvale.toLong
                    jsonetl.put("st", sss)
                  }
                }
                if (jsonetl.get("st") != null) {
                  val s = jsonetl.get("st").toString
                  if (StringUtils.isNumeric(s)) {
                    jsonetl.put("day", aldTimeUtil.st2Day(s.toLong))
                    jsonetl.put("hour", aldTimeUtil.st2hour(s.toLong))
                  }
                  else {
                    jsonetl.put("day", "null")
                    jsonetl.put("hour", "null")
                  }
                }

                //-------------------------------
                if (js.containsKey("ct") && js.get("ct") != null && js.get("ct") != "") {
                  if (js.get("ct").toString.contains("{")) {
                    val ct = js.get("ct").toString
                    //去掉ct字段中的反斜杠 /
                    val ctss = ct.replaceAll("\"", "\'")
                    jsonetl.put("ct", ctss)
                  } else if (js.get("ct").toString.substring(0, 1) == "\"") {
                    val ct = js.get("ct").toString
                    val cts: String = ct.replaceAll("\"", "")
                    jsonetl.put("ct", cts)
                  } else {
                    val ct = js.get("ct").toString
                    jsonetl.put("ct", ct)
                  }
                }
                if (!js.containsKey("ct")) {
                  jsonetl.put("ct", "null")
                }
                //去掉dr的双引号
                if (js.containsKey("dr") && js.get("dr") != null && !js.get("dr").equals("")) {
                  val drs = js.get("dr").toString
                  if (rule.isNumdr(drs)) {
                    val drss = drs.toLong
                    jsonetl.put("dr", drss)
                  } else {
                    jsonetl.put("dr", 0)
                  }
                }
                //处理其他字段 这些字段不要特殊处理 在filename 中添加就可以
                val fName: String = FieldName.ff
                val fields: Array[String] = fName.split(",")
                for (field <- fields) {
                  if (js.containsKey(field) && js.get(field) != null) {
                    jsonetl.put(field, js.get(field).toString)
                  } else {
                    jsonetl.put(field, "null")
                  }
                }
                jsonetl
              }
            }
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }

    }).filter(!_.toString.contains("()")).saveAsTextFile(FieldName.errpath + s"/$today/etl-$savePathName")


  }


  /**<br>gcs:<br>
    * 将从Flume爬取到的原始的json数据，进行第一次的清洗，之后再将清洗完的数据存储到ETL当中。 <br>
    * 这个清洗完的json数据，会被用来存储到pqrquet当中 <br>
    * */
  def etlHourJson(DS: DataFrame, aldTimeUtil: TimeUtil, today: String, savePathName: String) {

    //==========================================================5
    /*
    *gcs:
    *这个文件的目录是啥？？？
    * 是用来进行文件存储的吗
    */
    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)

    /*
    *gcs:
    *检测要存储的第一次ETL处理完的数据要存储在哪个路径下面
    * 我们的第一次Etl处理完成的数据的存储路径和最原始的ETL的数据存储的路径是完全一样的。
    * 只是第一次处理完成的etl的程序在存储路径的前面添加了etl这样的字段。"FieldName.etlpath/today/etl-todayhour" 的形式
    * 而最原始的etl的数据的类型是
    */
    if (fileSystem.exists(new Path(FieldName.etlpath + s"/$today/etl-$savePathName"))) {
      fileSystem.delete(new Path(FieldName.etlpath + s"/$today/etl-$savePathName"), true)
    }
    println("-----------etlpath------------")
    println(FieldName.etlpath + s"/$today/etl-$savePathName")


    //==========================================================6
    /*
    *gcs:
    *将传进来的dataFrame进行处理
    */
    DS.toJSON.rdd.map(line => {
      try {
        var jsonetl = new JSONObject()
        if (line != null) {
          //创建ak规则对象
          val akRule = new regex_rule //gcs:创建一个正则表达式对象
          //创建json对象存放处理的数据
          val js1 = JSON.parseObject(line) //gcs：line其实是String类型的json。这里的操作是将String类型的json再次转换为json
          val jsline = js1.get("value").toString //gcs:将文件当中的value提取出来
          val js = JSON.parseObject(jsline)   //gcs:将jsline在转换为json类型
          //==========================================================7
          /*
          *gcs:
          *这是对于ak的操作
          */
          if (js.containsKey("ak")) {
            val ak = js.get("ak").toString
            if (akRule.chenkak(ak)) { //gcs:如果ak是32位的数字
              //获得所对应的时间戳
              //==========================================================8
              /*
              *gcs:
              *对于ts字段的分析。把ts字段由String类型转换为long类型
              */
              if (js.containsKey("ts") && js.get("ts") != null && !js.get("ts").equals("")) {
                val stvale = js.get("ts").toString
                if (stvale.length == 13) {
                  val st = stvale.toLong
                  jsonetl.put("st", st)
                }
              }
              //==========================================================9
                /*
                *gcs:
                *对于st字段的分析。将st字段由String类型转换为long类型的数据
                */
              else if (js.containsKey("st") && js.get("st") != null && !js.get("st").equals("")) {
                val stvale = js.get("st").toString
                if (stvale.length == 13) {
                  val st = stvale.toLong
                  jsonetl.put("st", st)
                }
              }
              //==========================================================10
              /*
              *gcs:
              *判断st字段是否为Null
              * 如果st字段不为null，此时就会把day和hour的参数提取出来，存放到新创建的json当中
              */
              if (jsonetl.get("st") != null) {
                val st = jsonetl.get("st").toString
                if (StringUtils.isNumeric(st)) {
                  jsonetl.put("day", aldTimeUtil.st2Day(st.toLong))
                  jsonetl.put("hour", aldTimeUtil.st2hour(st.toLong))
                }
                else {
                  jsonetl.put("day", "null")
                  jsonetl.put("hour", "null")
                }
              }
              //==========================================================11
              /*
              *gcs:
              *处理ct字段
              */
              //处理ct
              if (js.containsKey("ct") && js.get("ct") != null) {
                val ct = js.get("ct").toString
                val err = "error"
                //gcs:将ct的字段全部转换为大写。这后判断ct当中是否包含，err的全部转换为大写的操作。
                //gcs:判断ct字段中是否有err类型的事件名字
                if (ct.toUpperCase.indexOf(err.toUpperCase) != -1) { //gcs: toUpperCase 将字符串中的所有的小写字符全部都转换为大写
                  jsonetl.put("error_messsage", ct) //gcs:如果ct字段中包含了ERROR这个类型，就会往json当中放入error_message这个错误信息
                  jsonetl.put("ct", "null") //gcs:此时就把ct字段赋值为null
                }
              }
              if (!js.containsKey("ct")) { //gcs:如果json当中不包含ct字段，此时就会把ct字段赋值为Null
                jsonetl.put("ct", "null")
              }
              //==========================================================12
              /*
              *gcs:
              *正常情况下的处理ct字段。
              */
              //-------------------------------
              if (js.containsKey("ct") && js.get("ct") != null && js.get("ct") != "") {
                if (js.get("ct").toString.contains("{")) { //gcs:如果ct字段中包含{
                  val ct = js.get("ct").toString
                  //去掉ct字段中的反斜杠 /
                  val s = ct.replaceAll("\"", "\'") //gcs:将ct字段从双引号变成单引号
                  jsonetl.put("ct", s)
                } else if (js.get("ct").toString.substring(0, 1) == "\"") { //gcs:如果ct字段的字符串中包含了双引号
                  val ct = js.get("ct").toString
                  val cts: String = ct.replaceAll("\"", "") //gcs:如果ct字段中有双引号，此时就可以把双引号变成空的字符串
                  jsonetl.put("ct", cts)
                } else { //gcs:否则把ct字段存放进去
                  val ct = js.get("ct").toString
                  jsonetl.put("ct", ct)
                }
              }
              //==========================================================13
              /*
              *gcs:
              *js字段是jsline字段的转换 <br>
              *js当中包含error_message，信息，此时就要把error_message的信息存放到jsonetl当中
              */
              //调整 ct 字段 变成一个嵌套json
              if (js.containsKey("error_messsage") && js.get("error_messsage") != null && js.get("error_messsage") != "") {
                val errorlog = js.get("error_messsage").toString
                jsonetl.put("error_messsage", errorlog)
              }
              //==========================================================14
              /*
              *gcs:
              *处理分享链的程序
              * ald_share_chain 是什么事件啊？？？
              */
              //处理分享链
              if (js.containsKey("tp") && js.get("tp") != null && js.get("tp").toString.equals("ald_share_chain")) {
                if (js.get("ct") != null && js.get("ct").toString.contains("{")) {
                  val ct = js.get("ct").toString
                  val s = ct.replaceAll("\"", "\'")  //gcs:将双引号变成单引号
                  val ctj = JSON.parseObject(s)
                  if (ctj.containsKey("path") && ctj.containsKey("chain")) { //gcs:如果ct字段中包含了path路径和chain，此时需要进行的操作
                    jsonetl.put("ct_path", ctj.get("path").toString)
                    jsonetl.put("ct_chain", ctj.get("chain").toString)
                  }
                }
              }
              //==========================================================15
              /*
              *gcs:
              *解析dr字段
              */
              //去掉dr的双引号
              if (js.containsKey("dr") && js.get("dr") != null && !js.get("dr").equals("")) {
                val drs = js.get("dr").toString
                if (akRule.isNumdr(drs)) {
                  val drss = drs.toLong
                  jsonetl.put("dr", drss)
                } else {
                  jsonetl.put("dr", 0)
                }
              }

              //==========================================================16
              /*
              *gcs:
              *将ag字段进行解析。
              */
              //去掉ag字段的反斜线
              if (js.containsKey("ag") && js.get("ag") != null && js.get("ct") != "") {
                if (js.get("ag").toString.contains("{")) { //gcs:当ag字段中还包含{ 号
                  val ct = js.get("ag").toString
                  //去掉ct字段中的反斜杠 /
                  val ctss = ct.replaceAll("\"", "\'") //gcs:将双引号变成单引号
                  jsonetl.put("ag", ctss)
                } else if (js.get("ag").toString.substring(0, 1) == "\"") { //gcs:将所有的双引号变成单引号
                  val ct = js.get("ag").toString
                  val cts: String = ct.replaceAll("\"", "")
                  jsonetl.put("ag", cts) //gcs:将ag放到原来的json目录当中
                } else {
                  val ct = js.get("ag").toString
                  jsonetl.put("ag", ct)
                }
              }
              //==========================================================17
              /*
              *gcs:
              *将所有的ag字段进行匹配
              */
              // 广告外链追踪
              if (js.containsKey("ag") && js.get("ag") != null && js.get("ag").toString.contains("{")) {
                val ct = js.get("ag").toString
                //将ct中的双引号和反斜线替换成单引号
                val s = ct.replaceAll("\"", "\'")  //gcs:将所有的双引号"，变成单引号
                val agj = JSON.parseObject(s)
                if (agj.containsKey("ald_link_key") && agj.containsKey("ald_position_id") && agj.containsKey("ald_media_id")) {
                  jsonetl.put("ag_ald_link_key", agj.get("ald_link_key").toString)
                  jsonetl.put("ag_ald_position_id", agj.get("ald_position_id").toString)
                  jsonetl.put("ag_ald_media_id", agj.get("ald_media_id").toString)
                }

              }
              else {
                jsonetl.put("ag_ald_link_key", "null")
                jsonetl.put("ag_ald_position_id", "null")
                jsonetl.put("ag_ald_media_id", "null")
              }
              //==========================================================18
              /*
              *gcs:
              *不需要进行处理的字段，直接放到我们的json当中就可以了。不需要在
              */
              //不需要处理的字段从FieldName中添加 需要处理的就不要放在filedName中了
              val fName: String = FieldName.ff   //gcs:这里做了一些字段的把关。之后当一定的值
              val fields: Array[String] = fName.split(",")
              for (field <- fields) {
                if (js.containsKey(field) && js.get(field) != null) {
                  jsonetl.put(field, js.get(field).toString)
                } else {
                  jsonetl.put(field, "null")
                }
              }
            }
            jsonetl
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })
      //==========================================================19
      /*
      *gcs:
      *将json不为Null,的重新存储到HDFS当中
      */
      .filter(!_.toString.contains("()")).saveAsTextFile(FieldName.etlpath + s"/$today/etl-$savePathName")

    /*
    *gcs:
    * hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070900
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070901
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070902
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070903
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070904
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070905
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070906
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070907
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070908
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070909
      hdfs://10.0.100.17:4007/ald_log_etl/20180709/etl-2018070910

      你看20180709 这个2018-07-09 日期下面还添加了01,02,03...10 这些数字是时间段，01小时,02小时

    */

  }


  /**<br>gcs:<br>
    * 将 etlHourJson 这个方法中的对于etl清洗过后的形成的新的etl。进行进一步的操作，将json数据转换为parquet数据。
    * 存储起来。让cos程序和conf程序接下来进行处理parquet的操作
    * */
  def parquetHourEtl(spark: SparkSession, aldTimeUtil: TimeUtil, today: String, hour: String) {

    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    //今天的日期加上小时用来判断处理某个小时的文件
    val th = today + hour
    //获取etl后的json路径

    //==========================================================20
    /*
    *gcs:
    *把 parquetHourEtl 方法产生的etl的文件读取出来
    */
    val paths: String = FieldName.etlpath
    val fs = fileSystem.listStatus(new Path(paths + today)) //gcs:获得文件的属性
    //读取到今天的etl-json路径下的所有文件
    val listPath = FileUtil.stat2Paths(fs) //gcs:将文件名的属性转换为文件的路径


    //==========================================================21
    /*
    *gcs:
    *将json文件的路径以此进行遍历
    */
    for (readPath <- listPath) {
      //for循环处理判读如果包含今天上一个小时的问题件夹则进行处理
      if (readPath.toString.contains(s"$th")) {
        //对文件路径进行切分 目的是为了存数据的文件夹名称
        val pathnames = readPath.toString.split("/") //gcs:将文件的路径提取出来
        //取切分后的最后一个为名称
        val pathname = pathnames(pathnames.length - 1)  //gcs:pathname为文件的路径
        try {

          //==========================================================22
          /*
          *gcs:
          *检测HDFS的目录下是否已经有了接下来在把json转换为parquet之后的，要存储进入的parquet的路径了。
          *如果已经有了要存储的这个parquet的路径了，就把这个路径先删除掉
          */
          if (fileSystem.exists(new Path(FieldName.parquetpath + s"$today/$pathname"))) {
            fileSystem.delete(new Path(FieldName.parquetpath + s"$today/$pathname"), true)
          }

          //==========================================================23
          /*
          *gcs:
          *从parquet当中把json数据读取出来。形成一个DF
          */
          val df = spark.read.option("mergeSchema", "true").json(s"$readPath")
          //            .repartition(100)

          //==========================================================24
          /*
          *gcs:
          *重新进行分区，将文件写到parquet当中
          */
          df.repartition(8).write.parquet(FieldName.parquetpath + s"$today/$pathname")
        } catch {
          case e: Exception => e.printStackTrace()
          case _: Exception => println("转parquet出错:" + readPath.toString)
        }

      }
    }

  }


  /**<br>gcs:<br>
    * 这个是将错误分析的的第一次ETL分析的程序生成的json转换为parquet的程序。<br>
    *即，将函数 errorEtlHourJson 的函数当中的对于原始的json数据的第一次的清洗产生的json数据的结果封装成parquet数据 <br>
    * */
  def errorParquetHourEtl(spark: SparkSession, aldTimeUtil: TimeUtil, today: String, hour: String) {


    //今天的日期加上小时
    val th = today + hour
    val conf = new Configuration()


    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    val paths: String = FieldName.errpath
    //读取到今天的 erroretl-json路径下的所有文件

    //==========================================================25
    /*
    *gcs:
    *创建一个HDFS的fileSystem的对象，用于操作HDFS的操作
    */
    val fs = fileSystem.listStatus(new Path(paths + today))
    val listPath = FileUtil.stat2Paths(fs)

    //==========================================================26
    /*
    *gcs:
    *将每一个目录进行遍历，之后将json数据从目录当中读取出来，之后再将这个json数据转换为parquet数据，存储在HDFS当中
    */
    for (d <- listPath) {
      //for循环处理判读如果包含今天上一个小时的问题件夹则进行处理
      if (d.toString.contains(s"$th")) {
        //对文件路径进行切分 目的是为了存数据的文件夹名称
        val pathnames = d.toString.split("/")
        //取切分后的最后一个为名称
        val pathname = pathnames(pathnames.length - 1)
        try {

          if (fileSystem.exists(new Path(FieldName.errparquet + s"$today/$pathname"))) {
            fileSystem.delete(new Path(FieldName.errparquet + s"$today/$pathname"), true)
          }

          val df = spark.read.option("mergeSchema", "true").json(s"$d")
          //            .repartition(100)
          df.repartition(8).write.parquet(FieldName.errparquet + s"$today/$pathname")
        } catch {
          case e: Exception => e.printStackTrace()
          case _: Exception => println("转parquet出错:" + d.toString)
        }
      }
    }
  }


  def eventETLHour(spark: SparkSession, aldTimeUtil: TimeUtil, today: String, hour: String) {

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

        val result = spark.sql(s"select ak,tp,udf(ss),time2date(st) from tmp$tablename").distinct()

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

              val ev_time = r(3)
              //如果数据库已经存在ev_id 这里只更新时间
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
      }
    }
  }

}




