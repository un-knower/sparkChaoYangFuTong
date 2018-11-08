package com.ald.stat.utils

/**<br>gcs:<br>
  * 定义全局变量
  * */
object GlobalConstants {
  //version
  val ALD_SDK_V_7 = "7.0.0"
  /**
    * 这里有一堆job 在redis中关键字
    */
  val JOB_REDIS_KEY_TREND = "Trend"
  val JOB_REDIS_KEY_SHARE = "Share"
  val JOB_REDIS_KEY_LINK = "Link"
  val JOB_REDIS_KEY_SCENE = "Scene"
  val JOB_REDIS_KEY_QR = "Qr"
  /**
    * 时间纬度redis关键字
    */
  val REDIS_KEY_DAILY = "Daily"
  val REDIS_KEY_HOURLY = "Hourly"
  /**
    * offset
    */
  val REDIS_KEY_STEP = "step"
  val REDIS_KEY_LATEST_OFFSET = "latest"

  /**
    * 指标redis关键字
    */
  val REDIS_KEY_SESSION = "SessionStat"
  val REDIS_KEY_PV_UV = "UVStat"

  val DONE_PARTITIONS = "donePartitions"

  val PATCH_TYPE_CACHE_LATEST = "cacheLatest" //补偿到上次实时失败时缓存中最大offset
  val PATCH_TYPE_REAL_LATEST = "realLatest" //补偿到真实最大offset到地方

}

