package com

import aldwxutil.{TimeUtil, regex_rule}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import property.FieldName

/**
  * Created by gaoxiang on 2017/12/11.
  */
object ETLHour {

  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.speculation", "true")
      .config("spark.sql.caseSensitive",true)
      .getOrCreate()
    val aldTimeUtil = new TimeUtil
    //获取今天时间
    val today = aldTimeUtil.processArgs(args)
    //获取当前小时的前一个小时
    val hour: String = aldTimeUtil.processArgsHour(args)
    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    //要读取的日志目录
    val paths: String = FieldName.jsonpath
    val fs = fileSystem.listStatus(new Path(paths + today))
    val listPath = FileUtil.stat2Paths(fs)
    for (readPath <- listPath) {
      if (readPath.toString.contains(s"$hour.json")) {
        val getSavePath = readPath.toString.split("/")
        val savePath = getSavePath(5)
        val savePathNames = savePath.split("\\.")
        val savePathName = savePathNames(0) + "-" + savePathNames(1)
        try {
          val DS = spark.read.text(s"$readPath")

          DS.toJSON.rdd.map(line => {
            try {
              var jsonetl = new JSONObject()
              if (line != null) {
                //创建ak规则对象
                val akRule = new regex_rule
                //创建json对象存放处理的数据
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
                        val st = stvale.toLong
                        jsonetl.put("st", st)
                      }
                    } else if (js.containsKey("st") && js.get("st") != null && !js.get("st").equals("")) {
                      val stvale = js.get("st").toString
                      if (stvale.length == 13) {
                        val st = stvale.toLong
                        jsonetl.put("st", st)
                      }
                    }
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
                    //处理ct
                    if (js.containsKey("ct") && js.get("ct") != null) {
                      val ct = js.get("ct").toString
                      val err = "error"
                      if (ct.toUpperCase.indexOf(err.toUpperCase) != -1) {
                        jsonetl.put("error_messsage", ct)
                        jsonetl.put("ct", "null")
                      }
                    }
                    if (!js.containsKey("ct")) {
                      jsonetl.put("ct", "null")
                    }
                    //-------------------------------
                    if (js.containsKey("ct") && js.get("ct") != null && js.get("ct") != "") {
                      if (js.get("ct").toString.contains("{")) {
                        val ct = js.get("ct").toString
                        //去掉ct字段中的反斜杠 /
                        val s = ct.replaceAll("\"", "\'")
                        jsonetl.put("ct", s)
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
                    //处理分享链
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
                    //去掉ag字段的反斜线
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
                    // 广告外链追踪
                    if (js.containsKey("ag") && js.get("ag") != null && js.get("ag").toString.contains("{")) {
                      val ct = js.get("ag").toString
                      //将ct中的双引号和反斜线替换成单引号
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
                    //不需要处理的字段从FieldName中添加 需要处理的就不要放在filedName中了
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
          }).filter(!_.toString.contains("()")).saveAsTextFile(FieldName.etlpath + s"/$today/etl-$savePathName")
        }
        catch {
          case e: Exception => e.printStackTrace()
          case _: Exception => println("出错路径---" + readPath.toString)
        }
      }
    }
    spark.stop()
  }
}
