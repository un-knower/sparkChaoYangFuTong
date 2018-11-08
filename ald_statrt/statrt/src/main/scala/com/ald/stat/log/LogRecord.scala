package com.ald.stat.log

import com.ald.stat.utils.GlobalConstants
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty

/**
  * ev="app" 、life:[launch|show|hide]
  *
  * 将日志的每一行转换成标准的对象，由于考虑到传递，所以需要序列化
  *
  */
class LogRecord extends Serializable {

  @BeanProperty
  var ak: String = _
  @BeanProperty
  var waid: String = _
  @BeanProperty
  var wst: String = _
  @BeanProperty
  var uu: String = _ //用户ID
  @BeanProperty
  var st: String = _ //app启动的时间 end time  13位时间戳
  @BeanProperty
  var ag: String = _ //上次访问的访问时间
  @BeanProperty
  var ag_ald_link_key: String = _
  @BeanProperty
  var ag_ald_media_id: String = _
  @BeanProperty
  var ag_ald_position_id: String = _
  @BeanProperty
  var wsr_query_ald_link_key: String = _
  @BeanProperty
  var wsr_query_ald_media_id: String = _
  @BeanProperty
  var wsr_query_ald_position_id: String = _
  @BeanProperty
  var ag_aldsrc: String = _
  @BeanProperty
  var at: String = _
  @BeanProperty
  var wsr: String = _
  @BeanProperty
  var ev: String = _ // 事件类型0:_trackPageView的事件类型；

  // 生命周期 ev="app" 、life:[launch|show|hide]
  //ev="page",life:[head|unload|]
  //ev ="" 其他
  @BeanProperty
  var life: String = _
  //小程序错误次数
  @BeanProperty
  var ec: String = _
  //网络类型
  @BeanProperty
  var nt: String = _
  //手机类型
  @BeanProperty
  var pm: String = _
  //微信基础框架版本
  @BeanProperty
  var wsdk: String = _
  // 手机屏幕像素点
  @BeanProperty
  var pr: String = _
  //手机屏幕款度
  @BeanProperty
  var ww: String = _
  //手机屏幕高度
  @BeanProperty
  var wh: String = _
  @BeanProperty
  var lang: String = _
  @BeanProperty
  var wv: String = _
  //地理位置-经度
  @BeanProperty
  var lat: String = _
  //地理位置-纬度
  @BeanProperty
  var lng: String = _
  //速度
  @BeanProperty
  var spd: String = _
  //SDK版本
  @BeanProperty
  var v: String = _
  //客户端操作系统版本
  @BeanProperty
  var sv: String = _
  //客户端平台
  @BeanProperty
  var wvv: String = _
  //是否首次打开小程序
  @BeanProperty
  var ifo: String = _
  @BeanProperty
  var city: String = _
  @BeanProperty
  var client_ip: String = _
  @BeanProperty
  var ct: String = _
  @BeanProperty
  var day: String = _
  @BeanProperty
  var dr: String = _
  @BeanProperty
  var error_messsage: String = _ //
  @BeanProperty
  var hour: String = _ ////
  @BeanProperty
  var ifp: String = _
  @BeanProperty
  var lp: String = _
  @BeanProperty
  var path: String = _
  @BeanProperty
  var pp: String = _
  @BeanProperty
  var province: String = _ //10位数以内的随机整数
  @BeanProperty
  var qr: String = _ // url
  @BeanProperty
  var scene: String = _ //refer
  @BeanProperty
  var scene_group_id: String = _
  @BeanProperty
  var et: String = _ //记录上报时间 13位时间戳
  @BeanProperty
  var tp: String = _
  @BeanProperty
  var wsr_query_ald_share_src: String = _
  @BeanProperty
  var wsr_query_aldsrc: String = _
  @BeanProperty
  var ct_chain: String = _
  @BeanProperty
  var ct_path: String = _

  /**
    * 品牌
    */
  @BeanProperty
  var brand: String = _

  /**
    * 型号
    */
  @BeanProperty
  var model: String = _

  /**
    * 分享源
    */
  @BeanProperty
  var src: String = _

  /**
    * 二维码组
    */
  @BeanProperty
  var qr_group: String = _

  override def clone(): LogRecord = {
    JSON.parseObject(JSON.toJSONString(this, true), classOf[LogRecord])
  }


  override def toString = s"LogRecord($ak, $waid, $wst, $uu, $st, $ag, $ag_ald_link_key, $ag_ald_media_id, $ag_ald_position_id, $wsr_query_ald_link_key, $wsr_query_ald_media_id, $wsr_query_ald_position_id, $ag_aldsrc, $at, $wsr, $ev, $life, $ec, $nt, $pm, $wsdk, $pr, $ww, $wh, $lang, $wv, $lat, $lng, $spd, $v, $sv, $wvv, $ifo, $city, $client_ip, $ct, $day, $dr, $error_messsage, $hour, $ifp, $lp, $path, $pp, $province, $qr, $scene, $scene_group_id, $et, $tp, $wsr_query_ald_share_src, $wsr_query_aldsrc, $ct_chain, $ct_path, $brand, $model, $src, $qr_group, $clone)"
}

object LogRecord extends Serializable {
  val logger = LoggerFactory.getLogger("LogRecord")

  def line2Bean(line: String): LogRecord = {
    try {
      val logRecord = JSON.parseObject(line, classOf[LogRecord])
      if (logRecord != null) {
        if (StringUtils.isBlank(logRecord.scene) && StringUtils.isNotBlank(logRecord.wsr)) {
          if (logRecord.wsr.isInstanceOf[String] && logRecord.wsr.startsWith("{") && logRecord.wsr.endsWith("}")) {
            val wsr = JSON.parseObject(logRecord.wsr, classOf[Wsr])
            logRecord.scene = wsr.scene //从wsr中获取scene字段

            // TODO: 关于path的逻辑，需要放到ETL进行处理。 
            //从wsr中获取path字段
            if (StringUtils.isNotBlank(wsr.path)) {
              logRecord.path = wsr.path
            }
            if (StringUtils.isNotBlank(logRecord.ct_path)) {
              //用于页面分享，如果ct_path不为null，则优先使用ct_path
              //ct_path 是新版(7.0)SDK的当前页面   path是进入小程序的起始页面   使用wsr.path计算页面分享是不正确的。
              //因为点击分享链接进来的，path中没有参数，所以这里只要路径，不要参数
              logRecord.path = logRecord.ct_path.split("\\?")(0)
            }

          }
        }
        if (!"true".equals(logRecord.ifo)) { //gcs:如果在转换过来的时候发现logRecord的ifo=null，就为它赋值为false。
          logRecord.ifo = "false"
        }
        //处理dr 毫秒=>秒
        if (StringUtils.isNotBlank(logRecord.dr)) {
          try {
            //7.0.0 版本需要进行处理毫秒到秒
            if (GlobalConstants.ALD_SDK_V_7 == logRecord.v) {
              logRecord.dr = (logRecord.dr.toLong / 1000).toString
            }
          } catch {
            case _: Throwable => {
              logRecord.dr = "0"
            }
          }
        }
        logRecord
      } else {
        null
      }
    }
    catch {
      case ex: Throwable => {
        logger.error(s"convert exeception $line", ex)
        null
      }
    }
  }


}
