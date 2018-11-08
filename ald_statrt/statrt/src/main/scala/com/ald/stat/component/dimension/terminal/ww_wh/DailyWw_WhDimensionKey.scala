package com.ald.stat.component.dimension.terminal.ww_wh

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils
import mx4j.log.LoggerBroadcaster.LoggerNotifier

/**
  * 终端分析——像素比分析
  * ww：屏幕宽度
  * wh：屏幕高度
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyWw_WhDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.ww +"_"+logRecord.wh
}

object DailyWw_WhDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyWw_WhDimensionKey(c)
}










