
  package com.ald.stat.component.dimension.qrCode.qrCode

import com.ald.stat.component.dimension.DimensionKey
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.ComputeTimeUtils

  /**
    * 二维码统计 DimensionKey定义
    * Created by admin on 2018/6/6.
    */
  class HourQrDimensionKey(logRecord: LogRecord) extends DimensionKey(logRecord) {
    var dayAndHour = ComputeTimeUtils.getDateStrAndHour(logRecord.et.toLong)
    override val key: String = logRecord.ak + ":" + dayAndHour._1 + ":" + dayAndHour._2 + ":" + logRecord.qr
  }




