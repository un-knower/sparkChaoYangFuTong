package com.ald.stat.component.dimension.trend

import com.ald.stat.component.dimension._
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * Key  ak:yyyymmdd:hh:at
  */
class HourSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.at
}

object HourSessionSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): HourSessionSubDimensionKey = new HourSessionSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}



