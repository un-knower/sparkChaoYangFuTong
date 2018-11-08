package com.ald.stat.component.dimension.terminal.wsdk

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 终端分析——基础库版本
  * lang：客户端基础库版本
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyWsdkDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.wsdk
}

object DailyWsdkDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyWsdkDimensionKey(c)
}










