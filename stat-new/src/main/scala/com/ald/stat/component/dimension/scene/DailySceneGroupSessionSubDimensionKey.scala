package com.ald.stat.component.dimension.scene

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}

class DailySceneGroupSessionSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord: LogRecord) {
  val dayHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong);
  override val key: String = logRecord.ak + ":" + dayHour._1 + ":" + logRecord.scene_group_id + ":" + logRecord.at
}


object DailySceneGroupSessionSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new DailySceneGroupSessionSubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailySceneGroupDimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}


