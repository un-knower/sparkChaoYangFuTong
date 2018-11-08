package com.ald.stat.module.session

import com.ald.stat.component.stat.SessionStat
import com.ald.stat.utils.GlobalConstants

object TrendHourlySessionStat extends SessionStat {
  override val name: String = s"${GlobalConstants.JOB_REDIS_KEY_TREND}${GlobalConstants.REDIS_KEY_HOURLY}${GlobalConstants.REDIS_KEY_SESSION}"
}
