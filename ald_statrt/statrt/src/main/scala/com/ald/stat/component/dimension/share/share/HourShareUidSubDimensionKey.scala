package com.ald.stat.component.dimension.share.share

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * 分享概况  UidSubmensionKey定义
  * tp = "ald_share_click"
  * @param logRecord
  */
class HourShareUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2  + ":" +logRecord.uu
}

object HourShareUidSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourShareDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new HourShareUidSubDimensionKey(c)
}















