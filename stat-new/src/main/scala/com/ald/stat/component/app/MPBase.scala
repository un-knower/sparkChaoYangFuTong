package com.ald.stat.component.app

import com.ald.stat.utils.Copyable

import scala.beans.BeanProperty

trait MPBase extends Serializable with Copyable {

  @BeanProperty
  var appKey: String = _
  @BeanProperty
  var dateStr: String = _

  override def toString = s"SiteBase(siteId=$appKey, dateStr=$dateStr)"

}
