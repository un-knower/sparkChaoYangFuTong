package com.ald.stat.component.dimension.share.share

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

/**
  * 分享概况    DimensionKey定义
  * tp = "ald_share_click" 标识用户主动分享事件
  * 用于计算分享新增人数 and 回流量
  * @param logRecord 日志对象
  */
class DailyShareDimensionKey(logRecord:LogRecord) extends DimensionKey(logRecord){
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1
}

object DailyShareDimensionKey extends KeyTrait[LogRecord,DimensionKey]{

  override def getKey(c: LogRecord): DimensionKey = new DailyShareDimensionKey(c)
}

