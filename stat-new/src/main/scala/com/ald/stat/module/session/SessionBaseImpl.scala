package com.ald.stat.module.session

import com.ald.stat.component.session.{SessionBase, SessionTrait}
import com.ald.stat.log.LogRecord
import com.ald.stat.utils.StrUtils


class SessionBaseImpl extends Serializable

/**
  *
  */
object SessionBaseImpl extends SessionTrait[LogRecord, SessionBase] {
  /**
    * 获得Key的实例，一般用于Object
    *
    * @param c
    * @return 返回Key的对象
    */
  override def getEntity(c: LogRecord): SessionBase = {
    val sessionBase = new SessionBase
    sessionBase.pages = 1
    sessionBase.pageCount = 1
    sessionBase.ev = c.ev;

    if (c.ifo == "true")
      sessionBase.newUser = 0
    else
      sessionBase.newUser = 1

    sessionBase.session = c.at;
    if (c.et != null)
      sessionBase.startDate = c.et.toLong
    sessionBase.firstUrl = c.pp;
    sessionBase.url = c.pp
    if (c.dr != null && StrUtils.isInt(c.dr))
      sessionBase.sessionDurationSum = c.dr.toLong
    sessionBase
  }
}
