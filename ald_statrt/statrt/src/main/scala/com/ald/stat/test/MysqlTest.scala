package com.ald.stat.test

import com.ald.stat.utils.DBUtils.getConnection

object MysqlTest extends App {

  val conn = getConnection();
  val stat = conn.createStatement();
  println(stat.executeQuery("select 1").first())

}
