package com.ald.stat.component.dimension.scene

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}



class HourSceneGroupUidSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 +  ":" + dayHour._2 + ":" + logRecord.scene_group_id  + ":" +logRecord.uu
}

object HourSceneGroupUidSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourSceneGroupDimensionKey(k.lr)

  /**
    * 获得纬度key中hh:uu
    *
    * @param k
    * @return
    */
  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new HourSceneGroupUidSubDimensionKey(c)

}

