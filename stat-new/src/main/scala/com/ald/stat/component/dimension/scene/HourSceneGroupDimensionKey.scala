package com.ald.stat.component.dimension.scene

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

class HourSceneDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord: LogRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2 + ":" + logRecord.scene_group_id + ":" + logRecord.scene
}


object HourSceneDimensionKey extends KeyTrait[LogRecord, DimensionKey] {
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord) = new HourSceneDimensionKey(c)
}

class HourSceneGroupDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord: LogRecord) {
  var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
  override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2 + ":" + logRecord.scene_group_id
}


object HourSceneGroupDimensionKey extends KeyTrait[LogRecord, DimensionKey] {
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord) = new HourSceneGroupDimensionKey(c)
}
