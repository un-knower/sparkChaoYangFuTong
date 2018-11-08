package com.ald.stat.module.entrancePage

import com.ald.stat.log.LogRecord


class EntrancePageSessionImpl extends Serializable {

}

/**
  *
  */
object EntrancePageSessionImpl extends EntrancePageSessionTrait[LogRecord, EntrancePageSessionBase] {

  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getEntity(c: LogRecord): EntrancePageSessionBase = {
    val sessionBase = new EntrancePageSessionBase
    sessionBase.uv=1
    sessionBase.pageCount = 1
    sessionBase.pages = 1
    sessionBase.openCount = 1
    if (c.et != null)
      sessionBase.startDate = c.et.toLong
    sessionBase.firstUrl = c.pp
    sessionBase.url = c.pp
    if (c.dr != null)
      sessionBase.sessionDurationSum = c.dr.toLong
    sessionBase
  }
}
