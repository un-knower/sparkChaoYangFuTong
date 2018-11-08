package com.ald.stat.component.dimension.trend

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
  * 通用的维度key
  * ak:yyyymmdd:hh
  *
  * @param logRecord 日志对象
  */
////////////小时级别最基本的抽象类，包括当日和站点
class HourDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord: LogRecord) {

  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2

  override lazy val hashKey: String = HashUtils.getHash(this.key).toString

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      false
    }
    else
      obj.asInstanceOf[DimensionKey].key.equalsIgnoreCase(key)
  }

  override def hashCode() = key.hashCode()

  override def toString: String = key
}

object HourDimensionKey extends KeyTrait[LogRecord, DimensionKey] {
  implicit val dimensionKeySorting = new Ordering[DimensionKey] {
    override def compare(x: DimensionKey, y: DimensionKey): Int = x.key.compare(y.key)
  }

  def getKey(logRecord: LogRecord): DimensionKey = new HourDimensionKey(logRecord)
}

