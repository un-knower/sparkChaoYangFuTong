package com.ald.stat.component.dimension.link.media

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 媒体分析
  * Daily Dimension key  天
  * Created by admin on 2018/5/27.
  */
class DailyMediaDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1+ ":" + logRecord.wsr_query_ald_media_id
}

object DailyMediaDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyMediaDimensionKey(c)
}


