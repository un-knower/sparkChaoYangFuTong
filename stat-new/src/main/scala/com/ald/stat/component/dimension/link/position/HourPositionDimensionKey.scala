package com.ald.stat.component.dimension.link.position

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 位置分析
  * Daily Dimension key  天
  * Created by admin on 2018/5/27.
  */
class HourPositionDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1+ ":" + dayAndHour._2+ ":" + logRecord.wsr_query_ald_position_id
}

object HourPositionDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new HourPositionDimensionKey(c)
}





