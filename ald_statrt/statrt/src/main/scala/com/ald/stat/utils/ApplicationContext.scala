package com.ald.stat.utils

import org.apache.spark.SparkContext

import scala.beans.BeanProperty

class ApplicationContext {
  @BeanProperty
  var sparkContext:SparkContext = _

}

object ApplicationContext {
  val applicationContext = new ApplicationContext
}


