package com.ald.stat.test

import com.ald.stat.log.LogRecord

object TestLogRecord extends App {

  val x = "{\"ev\":\"page\",\"client_ip\":\"180.116.91.156\",\"ak\":\"e6259ee000da46fefaa3664b183dad3a\",\"referrerinfo\":{\"appId\":\"wxbe2cccd81f38841e\"},\"scene\":1038,\"pp\":\"pages\\/main\\/main\",\"relaunch\":false,\"pr\":\"3\",\"pm\":\"vivo X21A\",\"ec\":\"0\",\"spd\":\"undefined\",\"wsr\":\"{\\\"relaunch\\\":false,\\\"referrerInfo\\\":{\\\"appId\\\":\\\"wxbe2cccd81f38841e\\\"},\\\"scene\\\":1038,\\\"reLaunch\\\":false,\\\"query\\\":{\\\"navigateto\\\":\\\"lxwz\\\"},\\\"path\\\":\\\"pages\\/main\\/main\\\"}\",\"st\":\"1528012348734\",\"province\":\"江苏\",\"life\":\"show\",\"v\":\"6.1.2\",\"ag\":\"{\\\"navigateto\\\":\\\"lxwz\\\"}\",\"sc\":\"undefined\",\"lp\":\"pages\\/main\\/main\",\"lua_wechat_version\":\"6.6.6.1300(0x26060637)\",\"ag_navigateto\":\"lxwz\",\"dr\":\"4\",\"ww\":\"360\",\"lang\":\"zh_CN\",\"at\":\"15280123487231627952\",\"country\":\"中国\",\"uu\":\"15280108008813981672\",\"city\":\"常州\",\"server_time\":1528012348.27,\"query\":{\"navigateto\":\"lxwz\"},\"wh\":\"656\",\"wvv\":\"android\",\"wsr_query_navigateto\":\"lxwz\",\"nt\":\"wifi\",\"lat\":\"undefined\",\"lng\":\"undefined\",\"path\":\"pages\\/main\\/main\",\"wsdk\":\"2.0.9\",\"wv\":\"6.6.6\",\"rq_c\":\"13\",\"sv\":\"Android 8.1.0\"}"

  val l = LogRecord.line2Bean(x)
  println(l.pp)
}
