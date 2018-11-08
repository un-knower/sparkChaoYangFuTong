package com.ald.stat.component.dimension.phoneModel

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * 机型分析-机型
  * SessionSubDimensionKey
  * @param logRecord
  */
class DailyModelSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.model + ":" +logRecord.at
}

object DailyModelSessionSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getKey(c: LogRecord): SubDimensionKey = new DailyBrandSessionSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyModelDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

}









