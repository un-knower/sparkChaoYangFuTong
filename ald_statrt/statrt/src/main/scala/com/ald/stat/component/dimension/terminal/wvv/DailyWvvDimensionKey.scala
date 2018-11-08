package com.ald.stat.component.dimension.terminal.wvv

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 终端分析——客户端平台分析
  * wvv：客户端平台
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyWvvDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.wvv
}

object DailyWvvDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyWvvDimensionKey(c)
}




