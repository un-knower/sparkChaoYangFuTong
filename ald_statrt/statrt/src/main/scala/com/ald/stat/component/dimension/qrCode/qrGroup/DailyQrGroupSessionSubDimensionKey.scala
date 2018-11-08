package com.ald.stat.component.dimension.qrCode.qrGroup

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * 二维码组统计 session SubDimensionKey定义
  * @param logRecord
  */
class DailyQrGroupSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.qr_group + ":" +logRecord.at
}

object DailyQrGroupSessionSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{
  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyQrGroupDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new DailyQrGroupSessionSubDimensionKey(c)
}











