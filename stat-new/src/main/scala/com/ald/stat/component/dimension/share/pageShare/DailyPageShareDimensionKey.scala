package com.ald.stat.component.dimension.share.pageShare

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 页面分享    DimensionKey定义
  * @param logRecord 日志对象
  */
class DailyPageShareDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.path
}

object DailyPageShareDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyPageShareDimensionKey(c)
}



