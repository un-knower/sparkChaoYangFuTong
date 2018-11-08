package com.ald.stat.utils

import scala.util.matching.Regex

/**
  * Created by zhaofw on 2018/8/14.
  */
object TestAPP {

  def main(args: Array[String]): Unit = {
    val str = "http://tongji.aldwx.com/publice/miniapp-trend\u0001\u0001\u0001http://tongji.aldwx.com/publice/miniapp-trend"
//    val str1 = "7.0.0"
//    println(str>=str1)
//
//    val regex = new Regex("""^\d+$""")
//    val testStr = "12423ljkjljl"

    import java.util.regex.Pattern
    val isInt: Boolean = Pattern.compile("""^\d+$""").matcher(str).find

    val urls = str.split("\u0001\u0001\u0001")

    println(urls.length)

  }




}
