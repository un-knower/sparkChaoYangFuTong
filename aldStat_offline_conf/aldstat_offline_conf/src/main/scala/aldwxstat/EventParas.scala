package aldwxstat

import java.security.MessageDigest

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, EmojiFilter, JdbcUtil}
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * create by clark 2018-06-25
  */
object EventParas {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "aldwxutils.Registers")
      .getOrCreate()

    ArgsTool.analysisArgs(args)
    val yesterday = ArgsTool.day
    val du = ArgsTool.du
    val rdd = ArgsTool.getLogs(args, ss, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev='event'and " +
      "tp !='ald_error_message' and tp !='ald_reachbottom' and tp !='ald_share_click' and tp !='ald_share_status' and " +
      "tp !='ald_share_user' and tp !='ald_share_chain' and tp !='ald_pulldownrefresh' and tp !='js_err' and " +
      "ak!='d6be9d076493c2c3f51b0dbf1ac5cab0'").cache()


    val rowrdd = getEventParam(rdd).filter(x=>x.length==5)
    val schema = StructType(List(
      StructField("ak", StringType, nullable = true),
      StructField("tp", StringType, nullable = true),
      StructField("uu", StringType, nullable = true),
      StructField("ct_key", StringType, nullable = true),
      StructField("ct_value", StringType, nullable = true)
    ))

    val paramDF = ss.createDataFrame(rowrdd, schema)
    paramDF.createTempView("paramDF")

    val resultDF = ss.sql("select ak,tp,ct_key,ct_value,count(uu) as pv,count(distinct uu) as uv,now() from paramDF group by ak,tp,ct_key,ct_value")

    insertToMysql(resultDF, yesterday, du)

    ss.close()
  }

  /**
    * @param ds : Dataset
    * @return RDD[Row]
    */
  def getEventParam(ds: Dataset[Row]): RDD[Row] = {

    val map: mutable.Map[String, String] = scala.collection.mutable.Map()

    val jSONRDD = ds.toJSON.rdd.map(line => {
      map.clear()
      val jsonLine = JSON.parseObject(JSON.parseObject(line.toString).toString)
      var str: String = null
      if (jsonLine.get("ak") != null && jsonLine.get("tp") != null) {
        val ak = jsonLine.get("ak").toString
        val tp = jsonLine.get("tp").toString
        val uu = jsonLine.get("uu").toString
        var a = ""
        var b = ""
        if (jsonLine.get("ct") != null) {
          val ct = jsonLine.get("ct").toString
          if (ct.substring(0, 1).equals("{")) {
            if (!ct.toString.equals("{}")) {
              val ctString = ct.toString.substring(1, ct.toString.length - 1)
              val splitComma = ctString.split(",") //{\"ald_link_key\":\"4c7030fa684dc931\"}",
              //"ct":"{'distributorsID':'zilin','scene':1007}
              for (i <- 0 until splitComma.size) {
                val splitColon = splitComma(i).split(":") //{\"ald_link_key\":\"4c7030fa684dc931\"}",
                if (splitColon.size > 1) {
                  //取出事件的名称"ct":"{\"ald_link_key\":\"4c7030fa684dc931\"}",
                  if (splitColon(0).contains("\'")) {
                    if (splitColon(0).length > 1)
                      a = splitColon(0).substring(1, splitColon(0).length - 1)
                  } else {
                    a = splitColon(0)
                  }
                  //取出事件的值
                  if (splitColon(1).contains("\'")) {
                    if (splitColon(1).length > 1)
                      b = splitColon(1).substring(1, splitColon(1).length - 1)
                  } else {
                    b = splitColon(1)
                  }
                  //事件key 和 value 通过 : 拼接
                  str = a + "." + b
                  //放到map中 通过 "."来拼接key
                  map.put(ak + "." + tp + "." + uu + "." + str, "0")
                } else {
                  //去除 冒号
                  if (splitComma(0).contains("\'")) {
                    if (splitComma(0).length > 1)
                      a = splitComma(0).substring(1, splitComma(0).length - 1)
                  } else {
                    a = splitComma(0)
                  }
                  if (splitComma(1).contains("\'")) {
                    if (splitComma(1).length > 1)
                      b = splitComma(1).substring(1, splitComma(1).length - 1)
                  } else {
                    b = splitComma(1)
                  }
                  str = a + "." + b
                  map.put(ak + "." + tp + "." + uu + "." + str, "0")
                }
              }
            } else {
              map.put(ak + "." + tp + "." + uu + "." + "ct.value_null", "0")
            }
          } else {
            //处理不包括{的 ct

            val a = "ct" + "." + jsonLine.get("ct")

            map.put(ak + "." + tp + "." + uu + "." + a, "0")
          }
        } else {
          val c = "ct" + "." + jsonLine.get("ct")
          map.put(ak + "." + tp + "." + uu + "." + c, "0")
        }
      }
      //变成Seq（k1，k2.。。）的字符串，去掉前后括号等
      map.keys.toString().substring(4, map.keys.toString().length - 1)
    }).flatMap(_.split(", ")) //统计每个map key的数量
    val rowRDD = jSONRDD.map(str => {
      val strs = str.split("\\.")
      if (strs.length == 5) {
        Row(strs(0), strs(1), strs(2), strs(3), strs(4))
      } else {
        println("strs length is not 5!!!!")
        Row()
      }
    })
    rowRDD
  }
  def insertToMysql(result: DataFrame, yesterday: String, du: String): Unit = {
    result.foreachPartition((row: Iterator[Row]) => {
      try {
        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        var sqlText = ""
        row.foreach(r => {
          val app_key = r.get(0).toString
          //ak
          val day = yesterday
          val ev_id = r.get(1).toString
          //tp
          val event_key = MessageDigest.getInstance("MD5").digest((app_key + ev_id).getBytes("UTF-8")).map("%02x".format(_)).mkString("")
          //udf(ss)
          val ev_paras_name = r.get(2).toString
          //ct_key
          var ev_paras_value = r.get(3).toString
          val ef = new EmojiFilter()
          //ct_value
          val isEmoji = ef.isSpecial(ev_paras_value)
          if (isEmoji) {
            ev_paras_value = ef.filterSpecial(ev_paras_value)
            println(s"包含表情和特殊字符${r.toString()} 去掉表情和特殊字符=> $ev_paras_value")
          }
          val ev_paras_pv = r.get(4).toString.toInt
          val ev_paras_uv = r.get(5).toString.toInt
          val update_at = r.get(6).toString
          if (du == "") {
            sqlText =
              s"""
                 |insert into aldstat_event_paras(app_key,day,ev_id,event_key,ev_paras_name,ev_paras_value,ev_paras_count,update_at,ev_paras_uv)
                 |values (?,?,?,?,?,?,?,?,?)
                 |ON DUPLICATE KEY UPDATE ev_paras_count=? ,ev_paras_uv=?,update_at= ?
                      """.stripMargin
            params.+=(Array[Any](app_key, day, ev_id, event_key, ev_paras_name, ev_paras_value, ev_paras_pv, update_at, ev_paras_uv, ev_paras_pv, ev_paras_uv, update_at))

          } else {
            sqlText =
              s"""insert into aldstat_${du}days_event_paras (app_key,day,ev_id,event_key,ev_paras_name,ev_paras_value,ev_paras_count,update_at,ev_paras_uv)
                 |values (?,?,?,?,?,?,?,?,?)
                 |ON DUPLICATE KEY UPDATE ev_paras_count=?,ev_paras_uv=?,update_at=?""".stripMargin
            params.+=(Array[Any](app_key, day, ev_id, event_key, ev_paras_name, ev_paras_value, ev_paras_pv, update_at, ev_paras_uv, ev_paras_pv, ev_paras_uv, update_at))
          }
        })
        JdbcUtil.doBatch(sqlText, params) //批量入库
      } catch {
        case e: Exception => e.printStackTrace()
      }

    })
  }
}
