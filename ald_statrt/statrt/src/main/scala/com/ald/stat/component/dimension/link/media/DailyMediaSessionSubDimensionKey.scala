package com.ald.stat.component.dimension.link.media

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


class DailyMediaSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.ag_ald_media_id + ":" + logRecord.at
}

object DailyMediaSessionSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyMediaDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new DailyMediaSessionSubDimensionKey(c)

}







