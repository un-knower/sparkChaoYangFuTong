package com.ald.stat.component.dimension.trend

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * Key  ak:yyyymmdd:at
  */
class DailySessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.at
}

object DailySessionSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): DailySessionSubDimensionKey = new DailySessionSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}

