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


    //==========================================================1
    /*
    *gcs:
    *创建一个HDFS的文件系统
    */
    val fileSystem = FileSystem.get(conf)


    //获取原始json路径
    val paths: String = FieldName.jsonpath
    //==========================================================2
    /*
    *gcs:
    *把文件路径的状态显示出来
    */
    val fs = fileSystem.listStatus(new Path(paths + yesterday))
    //所有需要处理的数据路径
    val listPath = FileUtil.stat2Paths(fs) //gcs:将需要处理的文件的路径计算出来

    for (readPath <- listPath) {
      println(readPath)

      //==========================================================3
      /*
      *gcs:
      *因为待会儿处理完成的文件的路径和要重新进行存储的路径是完全相同的
      *所以这里要把文件的路径提取出来
      */
      //获取存放数据文件夹名称
      val getSavePath = readPath.toString.split("/")
      val savePath = getSavePath(5)  //gcs:提取文件的存储路径
      val savePathNames = savePath.split("\\.")
      val savePathName = savePathNames(0) + "-" + savePathNames(1)


      //==========================================================4
      /*
      *gcs:
      *遍历文件路径中的每一个路径。之后把json数据从路径中提取出来，重新进行分区的操作
      */
      try{
      val DS: Dataset[Row] = ss.read.json(s"$readPath").repartition(300)

      DS.toJSON.rdd.map(line => {
        try {

          if (line != null) {
            if (line.contains("ald_error_message") || line.contains("js_err")) { //gcs:这个ald_error_message应该是一种事件类型吧
              //ak规则对象
              val akRule = new regex_rule //gcs:制定一个正则表达式的格式。
              val js1 = JSON.parseObject(line) //gcs:string类型转换成为阿里的json格式
              val jsline = js1.get("value").toString //gcs:获得value的值


              val js = JSON.parseObject(jsline)  //gcs:再把jsline转换成为json

              if (js.containsKey("ak")) {
                val ak = js.get("ak").toString
                if (akRule.chenkak(ak)) { //gcs:如果ak是32位的。并且全部都由字母和数字组成。
                  var jsonetl = new JSONObject()

                  //==========================================================5
                  /*
                  *gcs:
                  *判断ts和ts的字段是否存在。
                  * 如果不存在，就会把st存放到新创建的jsonetl这个json对象当中去
                  */
                  //获得所对应的时间戳
                  if (js.containsKey("ts") && js.get("ts") != null && !js.get("ts").equals("")) {
                    val stvale = js.get("ts").toString
                    if (stvale.length == 13) {
                      val sss = stvale.toLong
                      jsonetl.put("st", sss)
                    }
                  }
                  //==========================================================6
                    /*
                    *gcs:
                    *检查st字段
                    */
                  else if (js.containsKey("st") && js.get("st") != null && !js.get("st").equals("")) {

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

                  //==========================================================7
                  /*
                  *gcs:
                  * 这个过程是对ct字段进行了调整
                  *一定要注意，在这个过程中，会把所有的ct字段中的双引号""，变成单引号''
                  */
                  //-------------------------------
                  if (js.containsKey("ct") && js.get("ct") != null && js.get("ct") != "") {
                    if (js.get("ct").toString.contains("{")) { //gcs:判断ct字段是否包含"{"字段。
                      val ct = js.get("ct").toString
                      //去掉ct字段中的反斜杠 /
                      val ctss = ct.replaceAll("\"", "\'") //gcs:将所有的双引号都变成单引号
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
                  if (!js.containsKey("ct")) { //gcs:如果检测到项目中不包含ct字段，此时要把ct字段添加到重组的ct字段当中
                    jsonetl.put("ct", "null")
                  }
                  //==========================================================8
                  /*
                  *gcs:
                  *这是对dr字段进行操作。
                  */
                  //去掉dr的双引号
                  if (js.containsKey("dr") && js.get("dr") != null && !js.get("dr").equals("")) { //gcs:如果js当中包含dr字段，并且dr字段不为Null.
                    val drs = js.get("dr").toString
                    if(akRule.isNumdr(drs)){ //gcs:如果dr字段都是数字
                      val drss = drs.toLong //gcs:把dr字段转换为long类型
                      jsonetl.put("dr", drss)
                    }else{
                      jsonetl.put("dr", 0)
                    }
                  }

                  //==========================================================9
                  /*
                  *gcs:
                  *将不用进行处理的字段都添加到ETL当中
                  */
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

      })
        //==========================================================10
        /*
        *gcs:
        *为什么要把这些数据存储到errpath当呢？？？？
        */
        .filter(!_.toString.contains("()")).saveAsTextFile(FieldName.errpath + s"/$yesterday/etl-$savePathName")
    }
    catch {
      case e: Exception => e.printStackTrace()
      case _:Exception=>println("出错路径---",println(readPath.toString()))

    }
    }
    ss.stop()
  }
}
