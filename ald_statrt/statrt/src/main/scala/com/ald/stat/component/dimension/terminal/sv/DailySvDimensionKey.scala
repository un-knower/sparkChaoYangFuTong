package com.ald.stat.component.dimension.terminal.sv

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 终端分析——操作系统版本分析
  * sv：操作系统版本
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailySvDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.sv
}

object DailySvDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailySvDimensionKey(c)
}










