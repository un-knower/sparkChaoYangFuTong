package com.ald.stat.component.dimension.share.pageShare

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * 页面分享  UidSubmensionKey定义
  * tp = "ald_share_click"
  * @param logRecord
  */
class DailyPageShareUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1  + ":" +logRecord.path + ":" +logRecord.uu
}

object DailyPageShareUidSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyPageShareDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new DailyPageShareUidSubDimensionKey(c)
}



















