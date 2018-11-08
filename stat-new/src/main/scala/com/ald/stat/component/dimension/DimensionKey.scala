package com.ald.stat.component.dimension

import com.ald.stat.log.LogRecord
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, HashUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 通用的维度key
  *
  * @param logRecord 日志对象
  */
////////////最基本的抽象类，包括当日和站点

class DimensionKey(logRecord: LogRecord) extends Serializable {

  val lr = logRecord
  val key: String = logRecord.ak + ":" + ComputeTimeUtils.getDateStr(logRecord.et.toLong)

  lazy val hashKey: String = HashUtils.getHash(this.key).toString
  //  lazy val hashKey: String = this.key

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      false
    }
    else
      obj.asInstanceOf[DimensionKey].key.trim.equalsIgnoreCase(key.trim)
  }

  override def hashCode() = key.hashCode

  override def toString: String = key
}

object DimensionKey extends KeyTrait[LogRecord, DimensionKey] {
  implicit val dimensionKeySorting = new Ordering[DimensionKey] {
    override def compare(x: DimensionKey, y: DimensionKey): Int = x.key.compare(y.key)
  }

  def getKey(logRecord: LogRecord): DimensionKey = new DimensionKey(logRecord)
}

////////////////////////////////////////////
class SubDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {

}

object SubDimensionKey extends KeyParentTrait[LogRecord, SubDimensionKey, DimensionKey] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecord): SubDimensionKey = new SubDimensionKey(c)

  override def getBaseKey(k: SubDimensionKey): DimensionKey = new DimensionKey(k.lr)

  override def getKeyofDay(k: SubDimensionKey): Int = HashUtils.getHash(k.key)
}


////////////////////////////////////////////
/**
  * 天气网Key的特质
  *
  * @tparam C
  * @tparam K
  */
trait KeyTrait[C <: LogRecord, K <: DimensionKey] {

  implicit val clientStatSorting = new Ordering[K] {
    override def compare(x: K, y: K): Int = x.key.compareTo(y.key)
  }

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  def getKey(c: C): K


  def saveRDD(rdd: RDD[AnyRef], path: String): Unit = {
    rdd.saveAsObjectFile(ConfigUtils.getProperty("alluxio.path") + path)
  }

  def readRdd(sc: SparkContext, path: String): RDD[AnyRef] = {
    sc.objectFile(ConfigUtils.getProperty("alluxio.path") + path)
  }
}

/**
  * 用于去掉子类的内容
  *
  * @tparam C
  * @tparam K
  * @tparam P
  */
trait KeyParentTrait[C <: LogRecord, K <: SubDimensionKey, P <: DimensionKey] extends KeyTrait[C, K] {

  def getBaseKey(k: K): P

  def getKeyofDay(k: K): Int
}

trait ValueTrait[C <: LogRecord] {
  def getValue(c: C): Long
}

