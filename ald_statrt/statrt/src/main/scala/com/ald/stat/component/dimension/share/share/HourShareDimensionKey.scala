package com.ald.stat.component.dimension.share.share

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 分享概况    DimensionKey定义
  * @param logRecord 日志对象
  */
class HourShareDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2
}

object HourShareDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new HourShareDimensionKey(c)
}



