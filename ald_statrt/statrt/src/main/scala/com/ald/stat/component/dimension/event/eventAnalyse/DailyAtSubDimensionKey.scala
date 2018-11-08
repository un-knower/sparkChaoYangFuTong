package com.ald.stat.component.dimension.event.eventAnalyse

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
  * Created by spark01 on 6/4/18.
  */
class DailyAtSubDimensionKey(logRecord:LogRecord) extends SubDimensionKey(logRecord){

  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)

  override val key = logRecord.ak + ":" + dayAndHour._1 + ":" + logRecord.tp + ":" +logRecord.at

}
object DailyAtSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{


  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyUuDimensionKey(k.lr)


  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new DailyAtSubDimensionKey(c)
}
