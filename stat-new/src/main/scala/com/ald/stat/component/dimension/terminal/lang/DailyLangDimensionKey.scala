package com.ald.stat.component.dimension.terminal.lang

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 终端分析——语言分析
  * lang：微信设置语言的种类
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyLangDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.lang
}

object DailyLangDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyLangDimensionKey(c)
}








