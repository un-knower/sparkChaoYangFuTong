package com.ald.stat.component.dimension.link.media

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 媒体分析
  * Daily Dimension key  天
  * Created by admin on 2018/5/27.
  */
class HourMediaDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2+ ":" + logRecord.ag_ald_media_id
}

object HourMediaDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new HourMediaDimensionKey(c)
}


