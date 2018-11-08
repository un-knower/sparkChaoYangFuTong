package com.ald.stat.component.dimension.trend

import com.ald.stat.component.dimension.{DimensionKey, KeyTrait}
import com.ald.stat.log.LogRecord

/**
  * Created by spark01 on 18-5-22.
  */
class OpenCountOfDailyDimensionKey(logRecord: LogRecord)  extends  DimensionKey(logRecord){


  var extraDimension =(logRecord.at) //gcs:额外的维度，在数组里面进行添加就可以了
  var extraDimensionSum :String= _
  for (item <- extraDimension){
    extraDimension += ":"+item
  }

//  override val key = super.key +extraDimensionSum  //gcs:改成数组的方式
}


object OpenCountOfDailyDimensionKey extends KeyTrait[LogRecord, DimensionKey]{
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): DimensionKey = new OpenCountOfDailyDimensionKey(c)
}