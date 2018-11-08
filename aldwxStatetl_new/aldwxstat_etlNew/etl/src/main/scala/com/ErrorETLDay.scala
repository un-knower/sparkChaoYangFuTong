package com

import aldwxutil.{TimeUtil, regex_rule}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import property.FieldName

/**
  * Created by gaoxiang on 2017/12/14.
  */
object ErrorETLDay {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.serilizer", "org.apache.spark.KryoSerilizer")
      .getOrCreate()
    val aldTimeUtil = new TimeUtil
    val yesterday = aldTimeUtil.processArgs2(args)
    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    //获取原始json路径
    val paths: String = FieldName.jsonpath
    val fs = fileSystem.listStatus(new Path(paths + yesterday))
    //所有需要处理的数据路径
    val listPath = FileUtil.stat2Paths(fs)
    for (readPath <- listPath) {
      println(readPath)
      //获取存放数据文件夹名称
      val getSavePath = readPath.toString.split("/")
      val savePath = getSavePath(5)
      val savePathNames = savePath.split("\\.")
      val savePathName = savePathNames(0) + "-" + savePathNames(1)
      try{
      val DS: Dataset[Row] = ss.read.json(s"$readPath").repartition(300)
      DS.toJSON.rdd.map(line => {
        try {

          if (line != null) {
            if (line.contains("ald_error_message") || line.contains("js_err")) {
              //ak规则对象
              val akRule = new regex_rule
              val js1 = JSON.parseObject(line)
              val jsline = js1.get("value").toString
              val js = JSON.parseObject(jsline)
              if (js.containsKey("ak")) {
                val ak = js.get("ak").toString
                if (akRule.chenkak(ak)) {
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
                    if(akRule.isNumdr(drs)){
                      val drss = drs.toLong
                      jsonetl.put("dr", drss)
                    }else{
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

      }).filter(!_.toString.contains("()")).saveAsTextFile(FieldName.errpath + s"/$yesterday/etl-$savePathName")
    }
    catch {
      case e: Exception => e.printStackTrace()
      case _:Exception=>println("出错路径---",println(readPath.toString()))

    }
    }
    ss.stop()
  }
}
