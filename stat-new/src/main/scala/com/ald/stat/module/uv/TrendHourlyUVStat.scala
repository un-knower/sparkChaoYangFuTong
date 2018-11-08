package com.ald.stat.module.uv

import com.ald.stat.component.stat.UV
import com.ald.stat.utils.GlobalConstants

/**
  * Created by root on 2018/5/22.
  */
object TrendHourlyUVStat extends UV {
  override val name: String = s"${GlobalConstants.JOB_REDIS_KEY_TREND}${GlobalConstants.REDIS_KEY_HOURLY}${GlobalConstants.REDIS_KEY_PV_UV}"
}
