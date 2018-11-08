package com.ald.stat.component.session

import com.ald.stat.utils.Copyable

import scala.beans.BeanProperty

class SessionSum extends Copyable with Serializable {

  @BeanProperty
  var pvs = 0l
  @BeanProperty
  var uvs = 0l
  @BeanProperty
  var ips = 0l
  @BeanProperty
  var pagesSum = 0l //页数和
  @BeanProperty
  var sessionCount = 0l //session数量
  @BeanProperty
  var sessionDurationSum = 0l //session和
  @BeanProperty
  var newUserCount: Long = 0l //新用户数
  //  @BeanProperty
  //  var total:Int =_   //访问次数
  @BeanProperty
  var onePageofSession: Long = 0l //在用户的一个session中只有一个页面的session数量
  var updateTime: Long = 0l //最后更新时间

  override def toString = s"SessionSum(pvs=$pvs, uvs=$uvs, ips=$ips, pagesSum=$pagesSum, sessionCount=$sessionCount, sessionDurationSum=$sessionDurationSum, newUserCount=$newUserCount, onePageofSession=$onePageofSession)"
}
