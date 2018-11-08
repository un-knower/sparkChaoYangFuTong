package com.ald.stat.component.dimension

import com.ald.stat.log.{LogRecord, LogRecordExtendSS}
import com.ald.stat.utils.{ComputeTimeUtils, ConfigUtils, HashUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by spark01 on 6/7/18.
  */
class DimensionKeyExtend(logRecordExtendSS: LogRecordExtendSS)  extends Serializable{

  val lr = logRecordExtendSS
  val key: String = logRecordExtendSS.ak + ":" + ComputeTimeUtils.getDateStr(logRecordExtendSS.st.toLong)

  lazy val hashKey: String = HashUtils.getHash(this.key).toString
  //  lazy val hashKey: String = this.key

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      false
    }
    else
      obj.asInstanceOf[DimensionKeyExtend].key.trim.equalsIgnoreCase(key.trim)
  }

  override def hashCode() = key.hashCode

  override def toString: String = key
}

object DimensionKeyExtend extends KeyTraitExtend[LogRecordExtendSS,DimensionKeyExtend]{

//  implicit val dimensionKeySorting = new Ordering[DimensionKeyExtend]{
//    override def compare(x: DimensionKeyExtend, y: DimensionKeyExtend): Int = x.key.compare(y.key)
//  }
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecordExtendSS): DimensionKeyExtend = new DimensionKeyExtend(c)

}


////////////////////////////////////////////
class SubDimensionKeyExtend(logRecordExtend: LogRecordExtendSS) extends DimensionKeyExtend(logRecordExtend) {

}
object SubDimensionKeyExtend extends KeyParentTraitExtend[LogRecordExtendSS,SubDimensionKeyExtend,DimensionKeyExtend]{
  override def getBaseKey(k: SubDimensionKeyExtend): DimensionKeyExtend = new DimensionKeyExtend(k.lr)

  override def getKeyofDay(k: SubDimensionKeyExtend): Int = HashUtils.getHash(k.key)

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getKey(c: LogRecordExtendSS): SubDimensionKeyExtend = new SubDimensionKeyExtend(c)
}





/**
  *
  *
  * @tparam C
  * @tparam Q
  */
trait KeyTraitExtend[C <: LogRecordExtendSS, Q <: DimensionKeyExtend] {

  implicit val clientStatSorting2 = new Ordering[Q] {
    override def compare(x: Q, y: Q): Int = x.key.compareTo(y.key)
  }

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  def getKey(c: C): Q


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
trait KeyParentTraitExtend[C <: LogRecordExtendSS, K <: SubDimensionKeyExtend, P <: DimensionKeyExtend] extends KeyTraitExtend[C, K] {

  def getBaseKey(k: K): P

  def getKeyofDay(k: K): Int

  def getRedisFieldValue(c: C): String=null
}

