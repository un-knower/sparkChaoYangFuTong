package com.ald.stat.module.entrancePage

import com.ald.stat.utils.Copyable

import scala.beans.BeanProperty

class EntrancePageSessionSum extends Copyable with Serializable {

  //入口页地址
  @BeanProperty
  var entrancePage: String = _
  //访问次数
  @BeanProperty
  var pv = 0l
  @BeanProperty
  var pages = 0
  //访问人数
  @BeanProperty
  var uv = 0l
  @BeanProperty
  var openCount = 0l
  //在用户的一个session中这个就访问这一个url的session个数
  @BeanProperty
  var onePageOfSessionSum: Long = 0l
  //url作为入口页的访问数量
  @BeanProperty
  var firstPageOfSessionSum: Long = 0l
  //页面停留
  var sessionDurationSum = 0l

}
