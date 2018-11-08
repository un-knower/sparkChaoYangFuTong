package com.ald.stat.component.dimension.depthAndDuration

import com.ald.stat.component.dimension.{DimensionKeyExtend, KeyTraitExtend}
import com.ald.stat.log.{LogRecord, LogRecordExtendSS}
import com.ald.stat.utils.ComputeTimeUtils

/**
  * Created by spark01 on 6/7/18.
  */
class DailyUVDepthDimensionKeyExtend(logRecordExtendSS: LogRecordExtendSS)  extends DimensionKeyExtend(logRecordExtendSS){

  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecordExtendSS.st.toLong)
  override val key = logRecordExtendSS.ak + ":" + dayAndHour._1 + ":" + logRecordExtendSS.pd
}

object DailyUVDepthDimensionKeyExtend extends KeyTraitExtend[LogRecordExtendSS,DimensionKeyExtend]  {
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecordExtendSS): DimensionKeyExtend = new DailyUVDepthDimensionKeyExtend(c)
}