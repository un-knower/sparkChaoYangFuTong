package com.ald.stat.component.dimension.phoneModel

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 机型分析-品牌
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyBrandDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.brand
}

object DailyBrandDimensionKey extends KeyTrait[LogRecord, DimensionKey] {
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): DimensionKey = new DailyBrandDimensionKey(c)

}
