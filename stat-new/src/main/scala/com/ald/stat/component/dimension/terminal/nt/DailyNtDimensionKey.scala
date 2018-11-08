package com.ald.stat.component.dimension.terminal.nt

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 终端分析——网络分析
  * nt：网络类型
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyNtDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.nt
}

object DailyNtDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyNtDimensionKey(c)
}






