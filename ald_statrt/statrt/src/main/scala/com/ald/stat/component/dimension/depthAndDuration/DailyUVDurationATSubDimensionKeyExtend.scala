package com.ald.stat.component.dimension.depthAndDuration

import com.ald.stat.component.dimension.{DimensionKeyExtend, KeyParentTraitExtend, SubDimensionKeyExtend}
import com.ald.stat.log.LogRecordExtendSS
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

/**
  * Created by spark01 on 6/8/18.
  */
class DailyUVDurationATSubDimensionKeyExtend(logRecordExtendSS: LogRecordExtendSS) extends SubDimensionKeyExtend(logRecordExtendSS){

  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecordExtendSS.st.toLong)
  override val key: String = logRecordExtendSS.ak + ":" + dayAndHour._1 + ":" + logRecordExtendSS.dd +
    ":" + logRecordExtendSS.at

}

object DailyUVDurationATSubDimensionKeyExtend extends  KeyParentTraitExtend[LogRecordExtendSS,SubDimensionKeyExtend,DimensionKeyExtend]{

  override def getBaseKey(k: SubDimensionKeyExtend): DimensionKeyExtend = new DailyUVDurationDimensionKeyExtend(k.lr)

  override def getKeyofDay(k: SubDimensionKeyExtend): Int = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecordExtendSS): SubDimensionKeyExtend = new DailyUVDurationATSubDimensionKeyExtend(c)


  /**<br>gcs:<br>
    * 生成用于Redis的Hash表中的field的值。使用ak+day+uu+at 判断session的唯一性
    * */
  override def getRedisFieldValue(c: LogRecordExtendSS): String = {

    val dayAndHour = ComputeTimeUtils.getDateStrAndHour(c.st.toLong)
    HashUtils.getHash(c.ak + ":" + dayAndHour._1 + ":" + c.uu + ":" + c.at).toString

  }

}
