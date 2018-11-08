package com.ald.stat.component.dimension.phoneModel

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 机型分析-机型
  * DailyDimensionKey   天
  * Created by admin on 2018/5/24.
  */
class DailyModelDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.model
}

object DailyModelDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  /**
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): DimensionKey = new DailyModelDimensionKey(c)
}


