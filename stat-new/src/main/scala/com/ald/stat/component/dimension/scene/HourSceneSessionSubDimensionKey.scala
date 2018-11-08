package com.ald.stat.component.dimension.scene

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

class HourSceneSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + dayHour._2 + ":" + logRecord.scene_group_id + ":" + logRecord.scene + ":" + logRecord.at
}

object HourSceneSessionSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new HourSceneSessionSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new HourSceneDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

}




