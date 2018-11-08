package com.ald.stat.module.uv

import com.ald.stat.component.stat.UV
import com.ald.stat.utils.GlobalConstants

object TrendDailyUVStat extends UV {
  override val name: String = s"${GlobalConstants.JOB_REDIS_KEY_TREND}${GlobalConstants.REDIS_KEY_DAILY}${GlobalConstants.REDIS_KEY_PV_UV}"
}
