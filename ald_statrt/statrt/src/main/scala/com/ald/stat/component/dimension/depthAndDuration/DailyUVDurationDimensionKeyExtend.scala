package com.ald.stat.component.dimension.depthAndDuration


import com.ald.stat.component.dimension.{DimensionKeyExtend, KeyTraitExtend}
import com.ald.stat.log.LogRecordExtendSS
import com.ald.stat.utils.ComputeTimeUtils

/**
  * Created by spark01 on 6/8/18.
  */
class DailyUVDurationDimensionKeyExtend(logRecordExtendSS: LogRecordExtendSS) extends DimensionKeyExtend(logRecordExtendSS){

  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecordExtendSS.st.toLong)
  override val key: String = logRecordExtendSS.ak + ":" + dayAndHour._1 + ":" + logRecordExtendSS.dd  //gcs:key = app_key+st的day时间+pd(访问时长的深度)
}

object DailyUVDurationDimensionKeyExtend extends  KeyTraitExtend[LogRecordExtendSS,DimensionKeyExtend]{
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecordExtendSS): DimensionKeyExtend = new DailyUVDurationDimensionKeyExtend(c)
}
