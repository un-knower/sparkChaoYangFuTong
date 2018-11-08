package com.ald.stat.component.dimension.link.position

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 位置分析
  * Daily Dimension key  天
  * Created by admin on 2018/5/27.
  */
class DailyPositionDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1+ ":" + logRecord.ag_ald_position_id
}

object DailyPositionDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyPositionDimensionKey(c)
}




