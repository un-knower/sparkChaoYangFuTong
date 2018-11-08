package com.ald.stat.component.dimension.link.link

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


class HourLinkUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1+ ":" + dayHour._2 + ":" + logRecord.ag_ald_link_key + ":" + logRecord.uu
}

object HourLinkUidSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourLinkDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new HourLinkUidSubDimensionKey(c)
}









