package com


import aldwxutil.{TimeUtil, regex_rule}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import property.FieldName

/**
  * Created by gaoxiang on 2017/12/5.
  */
object ETLDay {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.serilizer", "org.apache.spark.KryoSerilizer")
      .config("spark.speculation", true)
      .config("spark.sql.caseSensitive",true)
      .getOrCreate()
    val a = new TimeUtil
    //获取昨天的日期
    val yesterday = a.processArgs2(args)
    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    val paths: String = FieldName.jsonpath
    //获取昨天日期目录下的所有文件
    val fs = fileSystem.listStatus(new Path(paths + yesterday))
    val listPath = FileUtil.stat2Paths(fs)
    for (readPath <- listPath) {
      //for循环处理一个数据 并生成新的文件
      println(readPath)
      val getSavePath = readPath.toString.split("/")
      val savePath = getSavePath(5)
      val savePathNames = savePath.split("\\.")
      val savePathName = savePathNames(0) + "-" + savePathNames(1)
      try{
      val DS = ss.read.json(s"$readPath").repartition(300)
      DS.toJSON.rdd.map(line => {
        try {
          var jsonetl = new JSONObject()
          if (line != null) {
            //创建ak规则
            val akRule = new regex_rule

            //创建json对象
            val js1 = JSON.parseObject(line)
            val jsline = js1.get("value").toString
            val js = JSON.parseObject(jsline)
            if (js.containsKey("ak")) {
              val ak = js.get("ak").toString
              if (akRule.chenkak(ak)) {
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
                    jsonetl.put("day", a.st2Day(s.toLong))
                    jsonetl.put("hour", a.st2hour(s.toLong))
                  }
                  else {
                    jsonetl.put("day", "null")
                    jsonetl.put("hour", "null")
                  }
                }
                //处理ct
                if (js.containsKey("ct") && js.get("ct") != null) {
                  val ct = js.get("ct").toString
                  val err = "error"
                  if (ct.toUpperCase.indexOf(err.toUpperCase) != -1) {
                    jsonetl.put("error_messsage", ct)
                    jsonetl.put("ct", "null")
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
                //调整 ct 字段 变成一个嵌套json
                if (js.containsKey("error_messsage") && js.get("error_messsage") != null && js.get("error_messsage") != "") {
                  val errorlog = js.get("error_messsage").toString
                  jsonetl.put("error_messsage", errorlog)
                }
                //取出ag字段里面的嵌套json
                if (js.containsKey("tp") && js.get("tp") != null && js.get("tp").toString.equals("ald_share_chain")) {
                  if (js.get("ct") != null && js.get("ct").toString.contains("{")) {
                    val ct = js.get("ct").toString
                    val s = ct.replaceAll("\"", "\'")
                    val ctj = JSON.parseObject(s)
                    if (ctj.containsKey("path") && ctj.containsKey("chain")) {
                      jsonetl.put("ct_path", ctj.get("path").toString)
                      jsonetl.put("ct_chain", ctj.get("chain").toString)
                    }
                  }
                }
                if (!js.containsKey("ct")) {
                  jsonetl.put("ct", "null")
                }
                //                去掉ag字段的反斜线
                if (js.containsKey("ag") && js.get("ag") != null && js.get("ct") != "") {
                  if (js.get("ag").toString.contains("{")) {
                    val ct = js.get("ag").toString
                    //去掉ct字段中的反斜杠 /
                    val ctss = ct.replaceAll("\"", "\'")
                    jsonetl.put("ag", ctss)
                  } else if (js.get("ag").toString.substring(0, 1) == "\"") {
                    val ct = js.get("ag").toString
                    val cts: String = ct.replaceAll("\"", "")
                    jsonetl.put("ag", cts)
                  } else {
                    val ct = js.get("ag").toString
                    jsonetl.put("ag", ct)
                  }
                }

                //去掉dr的双引号
                if (js.containsKey("dr") && js.get("dr") != null && !js.get("dr").equals("")) {
                  val drs = js.get("dr").toString
                  if(akRule.isNumdr(drs)){
                    val drss = drs.toLong
                    jsonetl.put("dr", drss)
                  }else{
                    jsonetl.put("dr", 0)
                  }
                }
                // 广告外链追踪
                if (js.containsKey("ag") && js.get("ag") != null && js.get("ag").toString.contains("{")) {
                  val ct = js.get("ag").toString
                  val s = ct.replaceAll("\"", "\'")
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
                /**
                  * 处理其他字段 这些字段不要特殊处理 在filename 中添加就可以,
                  * 如果需要处理的字段filename中就不需要出现了
                  */

                val fName: String = FieldName.ff
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

      }).filter(!_.toString.contains("()")).saveAsTextFile(FieldName.etlpath + s"/$yesterday/etl-$savePathName")
    }
    catch {
      case e: Exception => e.printStackTrace()
      case _:Exception=>println("出错路径---",println(readPath.toString()))

    }
    }


    ss.stop()
  }
}
