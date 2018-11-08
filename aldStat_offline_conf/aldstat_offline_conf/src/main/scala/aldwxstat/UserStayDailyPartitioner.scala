package aldwxstat

import org.apache.spark.Partitioner

/**
  * Created by wangtaiyang on 2018/4/22. 
  */
class UserStayDailyPartitioner extends Partitioner {
  override def numPartitions: Int = 269 //这里定义的是一共要分多少个区


  override def getPartition(key: Any): Int = {
    //这里必须返回Int类型数字表示数据分到哪个分区，参数key表示数据中的key，
    //要自定义的RDD必须是keyvalue类型，如RDD[String,Int],此时参数key表示数据中的String那个key
    UserStayDailyPartitioner.autoIncrement()
  }

}

object UserStayDailyPartitioner {
  val records: Long = UserStayDaily_bak.records
  val everyRegions: Long = records / 270
  var regionRecords = 0L
  var partition = 0

  def autoIncrement(): Int = {
    synchronized({
      if (regionRecords < everyRegions)
        regionRecords += 1
      else {
        partition += 1
        regionRecords = 0L
      }
    })
    partition
  }
}
