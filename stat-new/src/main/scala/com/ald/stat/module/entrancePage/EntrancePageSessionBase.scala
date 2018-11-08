package com.ald.stat.module.entrancePage

import com.ald.stat.log.LogRecord

import scala.beans.BeanProperty

/*
*
* 一条记录的入口页临时实体
* */
class EntrancePageSessionBase {

  //一session个中所有的打开次数
  @BeanProperty
  var pageCount = 0l
  //一个session中访问的页面数量
  @BeanProperty
  var pages = 0
  @BeanProperty
  var sessionDurationSum = 0l
  @BeanProperty
  var startDate = 0l
  //一个session中访问的所有去重的url，格式：url1,url2
  @BeanProperty
  var url: String = _
  @BeanProperty
  var firstUrl: String = _ //入口页面
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

}


/**
  * 入口页Key的特质
  *
  * @tparam C
  * @tparam K
  */
trait EntrancePageSessionTrait[C <: LogRecord, K <: EntrancePageSessionBase] extends Serializable {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  def getEntity(c: C): K

}

