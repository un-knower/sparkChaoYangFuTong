package com.ald.stat.component.redis

object RedisKeys {
  val Delimiter = "\u0001\u0001\u0001"
  val RAW_LAST_KEY="RAW:RDD:LAST:KEY"  //最新更新的KEY
  val RAW_RDD_LIST_KEY="RAW:RDD:LIST:KEY" //按照时间排序的KEY，通过这个列表，可以重新恢复数据，真实的数据存入到alluxio

  val  RAW_LAST_TODAY_KEY=(dateStr:String)=>RAW_LAST_KEY+":"+dateStr
  val  RAW_RDD_TODAY_LIST_KEY=(dateStr:String)=>RAW_RDD_LIST_KEY+":"+dateStr

  def getAccessUrlBaseKey(dateStr :String, baseRedisKey :String ):String={
    s"$baseRedisKey:$dateStr:URL"
  }

  def getAccessDomainBaseKey(dateStr :String, baseRedisKey :String ):String={
    s"$baseRedisKey:$dateStr:DOMAIN"
  }
}
