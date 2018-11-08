package com.ald.stat.component.dimension.phoneModel

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * 机型分析-品牌
  * UidSubDimensionKey
  * @param logRecord
  */
class DailyBrandUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.brand + ":" +logRecord.uu
}

object DailyBrandUidSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getKey(c: LogRecord): SubDimensionKey = new DailyBrandUidSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyBrandDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

}





