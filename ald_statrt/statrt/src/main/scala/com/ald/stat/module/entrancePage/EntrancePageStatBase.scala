package com.ald.stat.module.entrancePage

import com.ald.stat.component.dimension.DimensionKey
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.control.Breaks._

trait EntrancePageStatBase extends Serializable {

  val Url_Delimiter = "\u0001\u0001\u0001"
  val Sum_Delimiter = "```"
  val Daily_Stat_Age = 3 * 24 * 3600

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
    * 1、如果r1 和 r2 at 相同，则两个进行比较合并，
    * 并与redis中的缓存比较，
    * 2、如果 r1和r2 at 不同，则r1和r2分别与相同session缓存的相比较
    * 3、
    *
    * @param r1
    * @param r2
    * @tparam P
    * @return
    */
  def mergeSession[P <: EntrancePageSessionBase : ClassTag](r1: P, r2: P): P = {
    //打开次数 ,pv
    r1.pageCount += 1
    //判断两条记录中的url
    val urlResult = checkInAccessUrl(r1.url, r2.url)
    if (!urlResult._1) {
      r1.pages = urlResult._3
      r1.url = urlResult._2
    }
    //入口页面
    if (r1.startDate > r2.startDate) {
      r1.startDate = r2.startDate
      if (!r1.firstUrl.equalsIgnoreCase(r2.firstUrl)) {
        r1.firstUrl = r2.firstUrl
      }
    }
    if (r1.sessionDurationSum < r2.sessionDurationSum) {
      r1.sessionDurationSum = r2.sessionDurationSum
    }
    r1
  }
}
