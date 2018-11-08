package com.ald.stat.component.dimension.trend

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.HashUtils

/**
  * Created by spark01 on 18-5-22.
  */
class DailyUUSubDimensionKey(logRecord: LogRecord) extends SubDimensionKey(logRecord) {


  var extraDimension = (logRecord.uu) //gcs:额外的维度，在数组里面进行添加就可以了
  var extraDimensionSum: String = _
  for (item <- extraDimension) {
    extraDimension += ":" + item
  }

  override val key = new DimensionKey(logRecord).key + extraDimensionSum //gcs:改成数组的方式

}


object DailyUUSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  override def getBaseKey(k: SubDimensionKey) = new DimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey) = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord) = new DailyUUSubDimensionKey(c)
}