package com.ald.stat.component.dimension.share.share

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * 分享概况  UidSubmensionKey定义
  * @param logRecord
  */
class DailyShareUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1  + ":" +logRecord.uu
}

object DailyShareUidSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyShareDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new DailyShareUidSubDimensionKey(c)
}













