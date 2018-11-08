package com.ald.stat.utils

import java.net.URL

object URLUtils {
  def getUrlNoParam(urlstr: String): String = {
    val test = urlstr.indexOf("?")
    if (test != -1) urlstr.substring(0, test)
    else urlstr
  }

  def getDomainFromUrl(urlstr: String): String = {
    try {
      val url = new URL(urlstr)
      val domain = url.getHost.toLowerCase
      domain
    }
    catch {
      case t: Throwable => {
        println(urlstr)
        t.printStackTrace()
        urlstr
      }
    }
  }
}
