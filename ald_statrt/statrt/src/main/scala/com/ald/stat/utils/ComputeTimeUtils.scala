package com.ald.stat.utils

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import com.ald.stat.DayUtils
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

object ComputeTimeUtils {

  //时间校正,差3秒钟将不校正
  def timesDiffence(serverTime: Long, clientTime: Long): Long = {
    val diff = toMillsecond(serverTime) - toMillsecond(clientTime)
    if (Math.abs(diff) < 3000) 0
    else
      diff
  }

  def toMillsecond(timestamp: Long): Long = {
    var timeLong = timestamp
    if (timestamp.toString.length == 10) timeLong = timestamp * 1000
    timeLong
  }


  def getDateStr(timestamp: Long): String = {
    val d = new Date(toMillsecond(timestamp))
    formatDate(d, null)
  }

  def getDateStrAndHour(timestamp: Long): (String, String) = {
    val d = new Date(toMillsecond(timestamp))
    (formatDate(d, null), formatDate(d, "HH"))
  }

  def getDateStrAndHm(timestamp: Long): (String, String) = {
    val d = new Date(toMillsecond(timestamp))
    (formatDate(d, null), formatDate(d, "HH:mm"))
  }

  def getDateStrByInt(date: String, days: Int): String = {
    val currentDay = DateUtils.parseDate(date, "yyyyMMdd")
    val d = DateUtils.addDays(currentDay, days)
    formatDate(d, null)
  }

  def formatDate(date: Date, pattern: String): String = {
    var formatDate = ""
    if (pattern != null && pattern.length > 0) formatDate = DateFormatUtils.format(date, pattern)
    else formatDate = DateFormatUtils.format(date, "yyyyMMdd")
    formatDate
  }

  def getYesterDayStr(dayStr: String): String = {
    DayUtils.getCalDays(dayStr, -1)
    //    LocalDate.parse(dayStr,DateTimeFormatter.BASIC_ISO_DATE).plusDays(-1).format(DateTimeFormatter.BASIC_ISO_DATE)
  }

  //gcs:1
  def getDateStr(date: Date): String = {
    DayUtils.formatDate(date, null)
    //    date.format(DateTimeFormatter.BASIC_ISO_DATE)
  }

  def getYesterDayTimeStamp(date: Date): Long = {
    DateUtils.truncate(DateUtils.addDays(date, -1), Calendar.DAY_OF_MONTH).getTime
    //    date.plusDays(-1).atTime(0,0,0).toEpochSecond(ZoneOffset.ofHours(8))*1000
  }

  def inDay(dayStr: String, eventTime: Long): Boolean = {
    val currentDay = DateUtils.parseDate(dayStr, "yyyyMMdd")
    //    val currentDay = LocalDate.parse(dayStr,DateTimeFormatter.BASIC_ISO_DATE)
    val currentDayStart = getTodayDayTimeStamp(currentDay)
    val tomorrowDatStart = getTomorowDayTimeStamp(currentDay)
    if (eventTime >= currentDayStart && eventTime < tomorrowDatStart) true
    else false
  }

  def getTomorowDayTimeStamp(date: Date): Long = {
    DateUtils.truncate(DateUtils.addDays(date, 1), Calendar.DAY_OF_MONTH).getTime
    //    date.plusDays(1).atTime(0,0,0).toEpochSecond(ZoneOffset.ofHours(8))*1000
  }

  def getTodayDayTimeStamp(date: Date): Long = {
    DateUtils.truncate(date, Calendar.DAY_OF_MONTH).getTime
    //    date.atTime(0,0,0).toEpochSecond(ZoneOffset.ofHours(8))*1000
  }

  def betweenStrDays(dateStartStr: String, dateEndStr: String) = {
    val dateStart = DateUtils.parseDate(dateStartStr, "yyyyMMdd")
    val dateEnd = DateUtils.parseDate(dateEndStr, "yyyyMMdd")
    betweenDays(dateStart, dateEnd)
  }

  def betweenDays(dateStart: Date, dateEnd: Date): Long = {
    try {
      val diff = dateStart.getTime - dateEnd.getTime
      (TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        0l
    }
  }

  /**
    * yyyyMMdd转时间戳
    * @param tm
    * @return
    */
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()/1000
    tim
  }

  def main(args: Array[String]): Unit = {
    println(tranTimeToLong("20180531"))
  }
}
