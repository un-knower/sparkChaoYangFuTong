package com.ald.stat.component.dimension.event.eventAnalyse

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * Created by spark01 on 6/4/18.
  */
class DailyUuDimensionKey(logRecord:LogRecord) extends  DimensionKey(logRecord){

  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)

  override val key = logRecord.ak + ":" +dayAndHour._1 + ":" + logRecord.tp
}

object DailyUuDimensionKey extends KeyTrait[LogRecord,DimensionKey]{
  /**
    * 获得该维度计算的baseKey，用于最后的所有的功能模块的规约
    *
    * @param c 源数据中的一条LogRecord数据
    * @return 返回建立的DimensionKey
    */
  override def getKey(c: LogRecord): DimensionKey = new DailyUuDimensionKey(c)

}
