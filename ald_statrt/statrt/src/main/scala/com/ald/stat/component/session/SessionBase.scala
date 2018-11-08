package com.ald.stat.component.session

import com.ald.stat.component.app.MPBase
import com.ald.stat.log.LogRecord

import scala.beans.BeanProperty

class SessionBase extends MPBase {

  @BeanProperty
  var ev: String = _ // 事件类型
  @BeanProperty
  var pageCount = 1
  //一个session中访问的页面数量
  @BeanProperty
  var pages = 1 //gcs:打开的页面个数
  @BeanProperty
  var pageDurationSum = 0l //页面停留时长总和
  @BeanProperty
  var sessionDurationSum = 0l //gcs:session的总时长
  @BeanProperty
  var startDate = 0l //gcs:session的开始时间
  @BeanProperty
  var endDate = 0l //gcs:session的结束时间

  //一个session中访问的所有去重的url，格式：(url1,url2....)
  @BeanProperty
  var url: String = _

  @BeanProperty
  var firstUrl: String = _ //入口页面
  @BeanProperty
  var session: String = _
  @BeanProperty
  var newUser: Int = _ // 0:新用户,1：老用户


  override def toString = s"SessionBase(siteId=$appKey, dateStr=$dateStr,eventType=$ev, pageCount=$pageCount, pages=$pages, pageDurationSum=$pageDurationSum, sessionDurationSum=$sessionDurationSum, startDate=$startDate, endDate=$endDate, url=$url, firstUrl=$firstUrl, session=$session, newUser=$newUser)"
}


////////////////////////////////////////////
/**
  * 天气网Key的特质
  *
  * @tparam C
  * @tparam K
  */
trait SessionTrait[C <: LogRecord, K <: SessionBase] extends Serializable {
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  def getEntity(c: C): K
}

