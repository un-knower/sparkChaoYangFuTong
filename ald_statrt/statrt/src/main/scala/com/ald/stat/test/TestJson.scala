package com.ald.stat.test

import com.alibaba.fastjson.JSONObject


object TestJson extends App {
  //  val x = "{\"ev\":\"page\",\"client_ip\":\"114.139.102.105\",\"ak\":\"33e12701c097f81a34f42f30fe4d8d76\",\"wh\":\"undefined\",\"lng\":\"undefined\",\"ag_topbarstatus\":false,\"ag_issticky\":false,\"pp\":\"undefined\",\"ag_path\":\"index.html\",\"pr\":\"undefined\",\"pm\":\"undefined\",\"ec\":\"0\",\"spd\":\"undefined\",\"wsr\":\"undefined\",\"st\":\"1526068368775\",\"province\":\"贵州\",\"life\":\"show\",\"v\":\"6.1.2\",\"ag\":\"{\\\"scene\\\":1089,\\\"path\\\":\\\"index.html\\\",\\\"query\\\":{},\\\"topBarStatus\\\":false,\\\"isSticky\\\":false}\",\"sc\":\"undefined\",\"ag_scene\":1089,\"dr\":\"7\",\"ww\":\"undefined\",\"at\":\"undefined\",\"uu\":\"15248552466445478637\",\"city\":\"黔南布依族苗族自治州\",\"server_time\":1526068367.572,\"lua_wechat_version\":\"6.6.1.1220(0x26060135)\",\"ag_query\":{},\"wvv\":\"undefined\",\"country\":\"中国\",\"nt\":\"wifi\",\"lat\":\"undefined\",\"lang\":\"undefined\",\"ifp\":\"true\",\"sv\":\"undefined\",\"wsdk\":\"undefined\",\"wv\":\"undefined\",\"rq_c\":\"5\"}"
  //  val logRecord = LogRecord.line2Bean(x)

  var j = new JSONObject()
  j.put("a", "o")
  println(j.toJSONString)
  println(j)

  //  if (logRecord != null &&
  //    StringUtils.isNotBlank(logRecord.ak) &&
  //    StringUtils.isNotBlank(logRecord.at)
  //    && StringUtils.isNotBlank(logRecord.ev) &&
  //    StringUtils.isNotBlank(logRecord.uu) &&
  //    StringUtils.isNotBlank(logRecord.et)
  //    && StringUtils.isNotBlank(logRecord.dr)
  //    && StringUtils.isNotBlank(logRecord.pp)
  ////    && StringUtils.isNotBlank(logRecord.scene)
  //  ) {
  //    println(logRecord)
  //  }

}
