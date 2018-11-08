package com.ald.stat.module.session

import com.ald.stat.component.stat.SessionStat
import com.ald.stat.utils.GlobalConstants

object TrendDailySessionStat extends SessionStat {
  override val name: String = s"${GlobalConstants.JOB_REDIS_KEY_TREND}${GlobalConstants.REDIS_KEY_DAILY}${GlobalConstants.REDIS_KEY_SESSION}"
}
