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
  * Created by gaoxiang on 2017/12/14.
  */
object ErrorETLHour {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.speculation","true")
      .config("spark.sql.caseSensitive",true)
      .getOrCreate()

    val a = new TimeUtil
    //获取今天时间
    val today = a.processArgs(args)
    //获取当前小时的前一个小时
    val hour: String = a.processArgsHour(args)
    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    //要读取的日志目录
    val paths: String = FieldName.jsonpath
    val fs = fileSystem.listStatus(new Path(paths + today))
    val listPath: Array[Path] = FileUtil.stat2Paths(fs)
    for (readPath <- listPath) {
      //for循环数组中的文件路径判断如果包含某个小时那就处理这个小时
      if (readPath.toString.contains(s"$hour.json")) {
        //获取存放数据文件夹名称
        val getSavePath = readPath.toString.split("/")
        val savePath = getSavePath(5)
        val savePathNames = savePath.split("\\.")
        val savePathName = savePathNames(0) + "-" + savePathNames(1)
        try{
        val DS = spark.read.json(s"$readPath")
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
                        jsonetl.put("day", a.st2Day(s.toLong))
                        jsonetl.put("hour", a.st2hour(s.toLong))
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
                      if(rule.isNumdr(drs)){
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

        }).filter(!_.toString.contains("()")).saveAsTextFile(FieldName.errpath + s"/$today/etl-$savePathName")
      }
      catch {
        case e: Exception => e.printStackTrace()
        case _:Exception=>println("出错路径---",println(readPath.toString()))

      }
      }
    }
    spark.stop()

  }
}
