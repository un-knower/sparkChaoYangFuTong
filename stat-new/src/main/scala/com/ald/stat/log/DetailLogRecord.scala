//package com.ald.stat.log
//
//import com.ald.stat.utils.ComputeTimeUtils
//import com.alibaba.fastjson.{JSON, JSONObject}
//import org.slf4j.LoggerFactory
//
//import scala.beans.BeanProperty
//
//
//class DetailLogRecord extends LogRecord with Serializable {
//
//  //  var countryId: String = _
//  @BeanProperty
//  var province_code: String = _
//  @BeanProperty
//  var city_code: String = _
//
//  @BeanProperty
//  var day: String = _
//
//  @BeanProperty
//  var hour: String = _
//
//  @BeanProperty
//  var orgId: String = _
//
//  @BeanProperty
//  var sd: String = _
//
//
//  override def clone(): DetailLogRecord = {
//    JSON.parseObject(JSON.toJSONString(this, true), classOf[DetailLogRecord])
//  }
//
//  override def toString = s"LogRecord(ag=$ag, ag_ald_link_key=$ag_ald_link_key, ag_ald_media_id=$ag_ald_media_id," +
//    s" ag_ald_position_id=$ag_ald_position_id, ag_aldsrc=$ag_aldsrc, ak=$ak, at=$at, city=$city, client_ip=$client_ip, " +
//    s"ct=$ct, day=$day, dr=$dr, error_messsage=$error_messsage, hour=$hour, ifo=$ifo, ifp=$ifp, lang=$lang, lp=$lp, " +
//    s"nt=$nt, path=$path, pm=$pm, pp=$pp, province=$province, ev=$ev, qr=$qr, scene=$scene, st=$st, " +
//    s"sv=$sv, tp=$tp, uu=$uu, v=$v, wh=$wh, wsdk=$wsdk ,wsr_query_ald_share_src=$wsr_query_ald_share_src," +
//    s"wsr_query_aldsrc=$wsr_query_aldsrc,wv=$wv,wvv=$wvv,ww=$ww,ct_chain=$ct_chain,ct_path=$ct_path)"
//
//}
//
//object DetailLogRecord extends Serializable {
//  val logger = LoggerFactory.getLogger("DetailLogRecord")
//
//  def line2Bean(line: String): DetailLogRecord = {
//    try {
//      val logRecord = JSON.parseObject(line, classOf[DetailLogRecord])
//      //访问时长,使用ck为访问时长
//      if (logRecord.ev == 3) {
//        try {
//          val jo = JSON.parseArray(logRecord.eventParams).get(0).asInstanceOf[JSONObject]
//          logRecord.st = jo.get("st").toString
//
//        }
//        catch {
//          case ex: Throwable => {
//            logger.error(s"convert exeception $line", ex)
//            logRecord.st = "0"
//          }
//        }
//
//      }
//      else {
//        logRecord.ck = 0
//      }
//      logRecord.st=((logRecord.st.toLong * 1000).toLong).toString
//      val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)
//      logRecord.day = dayAndHour._1
//      logRecord.hour = dayAndHour._2
//      logRecord.setFlash("all")
//      logRecord
//    }
//    catch {
//      case ex: Throwable => {
//        logger.error(s"convert exeception $line", ex)
//        null
//      }
//    }
//  }
//
//
//}
