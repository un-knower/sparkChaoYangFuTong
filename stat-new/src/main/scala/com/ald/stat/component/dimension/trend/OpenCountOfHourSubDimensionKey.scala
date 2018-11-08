package com.ald.stat.component.dimension.trend

import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord

/**
  * Created by spark01 on 18-5-22.
  */
class OpenCountOfHourSubDimensionKey(logRecord: LogRecord)  extends SubDimensionKey(logRecord ){




  var extraDimension =(logRecord.at)  //gcs:额外的维度，在数组里面进行添加就可以了
  var extraDimensionSum :String= _
  for (item <- extraDimension){
    extraDimension += ":"+item
  }

//  override val key = super.key +extraDimensionSum  //gcs:改成数组的方式




  def getParentKey: String = {
//    super.key
    ""
  }
}


object OpenCountOfHourSubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey]{


  override def getBaseKey(k: SubDimensionKey): DimensionKey = new OpenCountOfHourSubDimensionKey(k.lr) //gcs:这里只需要返回k不就可以了吗？不可以，这里是创建OpenCountOfHourSubDimensionKey类

  override def getKeyofDay(k: SubDimensionKey): Int = {
//    val key = k.key
//    if (key != null && key != "") {
//      val parts = key.split(":")
//      if (parts.length == 4)
//        return s"${parts(0)}:${parts(1)}:${parts(3)}"
//      else
//       return ""
//    } else
      1
  }

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
   override def getKey(c: LogRecord): SubDimensionKey ={

    new OpenCountOfHourSubDimensionKey(c)

  }



}