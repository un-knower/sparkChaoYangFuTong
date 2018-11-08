package com.ald.stat.component.dimension.event.eventAnalyse



import com.ald.stat.component.dimension.{DimensionKey, KeyParentTrait, SubDimensionKey}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, HashUtils}
/**
  * Created by spark01 on 6/4/18.
  */
class DailyUuSubDimensionKey(logRecord:LogRecord) extends SubDimensionKey(logRecord){

  val dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.st.toLong)
  override val key = logRecord.ak + ":" + dayAndHour._1 + ":" +logRecord.tp + ":" +logRecord.uu
}

object DailyUuSubDimensionKey extends KeyParentTrait[LogRecord,SubDimensionKey,DimensionKey]{


  /**
    *获得用户规约的baseKey的操作。在这里是获得了DailyUuDimensionKey
    * @param k 用于创建SubDimensionKey。该k在这里就是 DailyUuSubDimensionKey 的类型的
    * @return 返回一个DimensionKey，之后会通过这个DimensionKey获得key的值。在这里这个DimensionKey的类型是DailyUuDimensionKey
    * */
  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DailyUuDimensionKey(k.lr)


  /**
    * 获得SubDimensionKey的key的hash值
    * @param k 用于获得key的hash值的SubDimensionKey。在这里k的类型是DailyUuSubDimensionKey
    * @return 返回k.key的哈希值
    * */
  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)

  /**
    * 返回SubDimensionKey的操作。这里其实是获得DailyUuSubDimensionKey的操作
    *
    * @param c 用来创建SubDimensionKey的logRecord
    * @return 返回SubDimensionKey的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new DailyUuSubDimensionKey(c)
}
