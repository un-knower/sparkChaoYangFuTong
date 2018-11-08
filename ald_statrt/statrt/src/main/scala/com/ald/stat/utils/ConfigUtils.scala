package com.ald.stat.utils

import java.util.Properties

object ConfigUtils {
  //  var res = java.util.ResourceBundle.getBundle("test")
  val res = {
    val inputStream = getClass.getResourceAsStream("/app.properties")
    val properties = new Properties()
    properties.load(inputStream)
    properties
  };

  def getProperty(key: String) = res.getProperty(key)

  def main(args: Array[String]): Unit = {
    //    println(UUID.randomUUID())
    //    println(getProperty("scala.version"))
    //    println(getProperty("database.driver"))
    //    println(getProperty("database.user"))
    //    println(getProperty("kafka.wcsa.raw.topic"))
    println(getProperty("client.redis.write.pool"))
    println(getProperty("redis.write.pool"))
    println(getProperty("client.redis.write.pool.password"))

  }

}
