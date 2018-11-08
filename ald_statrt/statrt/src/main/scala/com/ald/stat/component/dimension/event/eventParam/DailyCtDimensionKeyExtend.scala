package com.ald.stat.component.dimension.event.eventParam


import com.ald.stat.component.dimension.{DimensionKeyExtend,KeyTraitExtend}
import com.ald.stat.log.{LogRecordExtendSS}
import com.ald.stat.utils.ComputeTimeUtils
/**
  * Created by spark01 on 6/6/18.
  */
class DailyCtDimensionKeyExtend(logRecordExtendSS: LogRecordExtendSS)  extends DimensionKeyExtend(logRecordExtendSS){

  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecordExtendSS.st.toLong)
  override val key = logRecordExtendSS.ak + ":" + dayAndHour._1 + ":" + logRecordExtendSS.tp + ":" + logRecordExtendSS.ss
}

object DailyCtDimensionKeyExtend extends KeyTraitExtend[LogRecordExtendSS,DimensionKeyExtend]{
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecordExtendSS): DimensionKeyExtend = new DailyCtDimensionKeyExtend(c)
}