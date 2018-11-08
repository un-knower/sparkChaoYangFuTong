package com.ald.stat.component.dimension.share.userShare

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}


/**
  * 用户分享  SessionSubmensionKey定义
  * tp = "ald_share_click"
  * src:分享来源，存储分享人的uu
  * @param logRecord
  */
class DailyUserShareSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1  + ":" +logRecord.src + ":" +logRecord.at
}

object DailyUserShareSessionSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyUserShareDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  override def getKey(c: LogRecord): SubDimensionKey = new DailyUserShareSessionSubDimensionKey(c)

}



















