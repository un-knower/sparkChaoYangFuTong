//package com.ald.stat.component.order
//
//import com.ald.stat.log.DetailLogRecord
//import com.ald.stat.log.DetailLogRecord
//
//class LogOrder(dateInt:Long) extends Ordered[DetailLogRecord] with Serializable {
//  override def compare(that: DetailLogRecord): Int = {
//    if(that.st.toLong > dateInt) 1
//    else -1
//  }
//}
