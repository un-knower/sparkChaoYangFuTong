package com.ald.stat.test

import com.ald.stat.log.LogRecord


object TestJson extends App {
  val x = "{\"ev\":\"event\",\"wsr_query_ald_share_src\":\"3e278616bd95d8e5faf9d86ad1632599\",\"referrerinfo\":{\"appId\":\"wxcff7381e631cf54e\",\"extraData\":{}},\"scene\":1037,\"tp\":\"ald_share_status\",\"wsr_query_options1\":\"1\",\"pr\":\"2.75\",\"decode_time\":0.042999999999793,\"pm\":\"Mi Note 3\",\"ip_time\":0,\"all_time\":1.071,\"lang\":\"zh_CN\",\"wsr\":\"{\\\"path\\\":\\\"pages\\/home\\/home\\\",\\\"query\\\":{\\\"name\\\":\\\"wangjie\\\",\\\"age\\\":\\\"20\\\",\\\"options1\\\":\\\"1\\\",\\\"ald_share_src\\\":\\\"3e278616bd95d8e5faf9d86ad1632599\\\"},\\\"scene\\\":1037,\\\"referrerInfo\\\":{\\\"appId\\\":\\\"wxcff7381e631cf54e\\\",\\\"extraData\\\":{}}}\",\"st\":\"1530625827960\",\"server_time\":1530625838272,\"op\":\"\",\"wsr_query_age\":\"20\",\"v\":\"7.0.0\",\"br\":\"Xiaomi\",\"at\":\"15306258279569432207\",\"wv\":\"6.6.7\",\"wsdk\":\"2.1.2\",\"waid\":\"appid\",\"spd\":\"-1\",\"sv\":\"Android 7.1.1\",\"ww\":\"392\",\"wh\":\"630\",\"wsr_query_name\":\"wangjie\",\"ak\":\"e72e3583be370cca09f9e00f210a7081\",\"ct\":\"{\\\"title\\\":\\\"北京海淀区西山国家森林公园\\\",\\\"path\\\":\\\"pages\\/details\\/details?name=wangjie&age=20&options1=1&ald_share_src=3e278616bd95d8e5faf9d86ad1632599\\\"}\",\"ct_path\":\"pages\\/details\\/details?name=wangjie&age=20&options1=1&ald_share_src=3e278616bd95d8e5faf9d86ad1632599\",\"se\":\"\",\"uu\":\"3e278616bd95d8e5faf9d86ad1632599\",\"ct_title\":\"北京海淀区西山国家森林公园\",\"wvv\":\"android\",\"path\":\"pages\\/home\\/home\",\"nt\":\"wifi\",\"lat\":\"39.976376\",\"oifo\":\"false\",\"et\":\"1530625836501\",\"wst\":\"appsecret\",\"rq_c\":\"4\",\"query\":{\"options1\":\"1\",\"age\":\"20\",\"name\":\"wangjie\",\"ald_share_src\":\"3e278616bd95d8e5faf9d86ad1632599\"},\"lng\":\"116.45845\"}"
  val logRecord = LogRecord.line2Bean(x)
  println(logRecord.path)


}
