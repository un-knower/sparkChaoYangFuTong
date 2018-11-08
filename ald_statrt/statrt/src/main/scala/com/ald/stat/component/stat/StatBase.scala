package com.ald.stat.component.stat

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.component.session.SessionBase
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.control.Breaks._

trait StatBase extends Serializable {

  val Url_Delimiter = "\u0001\u0001\u0001"
  val Sum_Delimiter = "```"
  val Daily_Stat_Age = 1 * 25 * 3600

  var Redis_Prefix: String = null


  def sum[K <: DimensionKey : ClassTag : Ordering](rdd1: RDD[(K, Long)], rdd2: RDD[(K, Long)]): RDD[(K, Long)] = {
    rdd1.union(rdd2).reduceByKey((x, y) => (x + y))
  }

  /**
    *
    * @param r1url 以分隔，如果相同返回true，如果false就增加url并返回
    * @param r2url
    * @return
    */
  def checkInAccessUrl(r1url: String, r2url: String): (Boolean, String, Int) = {
    val urls = r1url.split(Url_Delimiter)
    val length = urls.length
    var hasUrl = false
    breakable {
      urls.foreach(url => {
        if (url == r2url) {
          hasUrl = true
          break()
        }
      })
    }
    if (!hasUrl) {
      (hasUrl, r1url + Url_Delimiter + r2url, length + 1)
    }
    else
      (hasUrl, r1url, length)
  }

  /**
    *
    * @param r1
    * @param r2
    * @tparam P
    * @return
    */
  def mergeSession[P <: SessionBase : ClassTag](r1: P, r2: P): P = {
    if (r2.newUser == 0 || r1.newUser == 0)
      r1.newUser = 0
    else r1.newUser = 1


    r1.pageCount += 1
    val urlResult = checkInAccessUrl(r1.url, r2.url)
    if (!urlResult._1) {
      r1.pages = urlResult._3
      r1.url = urlResult._2
    }
    r1.setPageDurationSum(Math.abs(r1.startDate - r2.startDate)) //默认时长

    if (r1.startDate > r2.startDate) {
      r1.startDate = r2.startDate
      if (!r1.firstUrl.equalsIgnoreCase(r2.firstUrl)) {
        r1.firstUrl = r2.firstUrl //入口页面
      }
    }
    if (r1.endDate < r2.endDate) {
      r1.endDate = r2.endDate
    }
    if (r1.sessionDurationSum < r2.sessionDurationSum) {
      r1.sessionDurationSum = r2.sessionDurationSum
    }
    r1
  }
}
