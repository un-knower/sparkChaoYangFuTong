package com.ald.stat.component.dimension.trend

import com.ald.stat.component.dimension._
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * subKey  ak:yyyymmdd:hh:uu
  */
class HourUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord) {

  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.uu


}

object HourUidSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  override def getBaseKey(k: SubDimensionKey): HourDimensionKey = new HourDimensionKey(k.lr)

  /**
    * 获得纬度key中hh:uu
    *
    * @param k
    * @return
    */
  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new HourUidSubDimensionKey(c)
}