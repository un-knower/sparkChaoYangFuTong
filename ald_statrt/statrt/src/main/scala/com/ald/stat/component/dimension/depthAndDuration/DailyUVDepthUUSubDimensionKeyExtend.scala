package com.ald.stat.component.dimension.depthAndDuration

import com.ald.stat.component.dimension.{DimensionKeyExtend, KeyParentTraitExtend, SubDimensionKeyExtend}
import com.ald.stat.log.{LogRecord, LogRecordExtendSS}
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
  * Created by spark01 on 6/7/18.
  */
class DailyUVDepthUUSubDimensionKeyExtend(logRecordExtendSS: LogRecordExtendSS) extends SubDimensionKeyExtend(logRecordExtendSS){
  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecordExtendSS.st.toLong)
  override  val key = logRecordExtendSS.ak + ":" + dayAndHour._1 + ":" +
    logRecordExtendSS.pd + ":" +logRecordExtendSS.uu


}

object DailyUVDepthUUSubDimensionKeyExtend extends KeyParentTraitExtend[LogRecordExtendSS,SubDimensionKeyExtend,DimensionKeyExtend]{

  override def getBaseKey(k: SubDimensionKeyExtend): DimensionKeyExtend = new DailyUVDepthDimensionKeyExtend(k.lr)

  override def getKeyofDay(k: SubDimensionKeyExtend): Int = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecordExtendSS): SubDimensionKeyExtend = new DailyUVDepthUUSubDimensionKeyExtend(c)


  override def getRedisFieldValue(c: LogRecordExtendSS): String = {
    val dayAndHour = ComputeTimeUtils.getDateStrAndHour(c.st.toLong)

    val stringField = c.ak + ":" + dayAndHour._1 + ":" + c.uu + ":" + c.at +c.pp
    HashUtils.getHash(stringField).toString

  }
}