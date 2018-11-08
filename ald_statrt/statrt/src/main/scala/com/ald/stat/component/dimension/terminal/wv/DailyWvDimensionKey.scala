package com.ald.stat.component.dimension.terminal.wv

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 终端分析——微信版本号分析
  * wv：微信版本号
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyWvDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.wv
}

object DailyWvDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyWvDimensionKey(c)
}










