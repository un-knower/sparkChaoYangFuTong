package com.ald.stat.log

import scala.beans.BeanProperty

class Wsr {

  @BeanProperty
  var path: String = _
  @BeanProperty
  var scene: String = _
  @BeanProperty
  var query: String = _

  override def toString = s"Wsr($path, $scene, $query)"
}
