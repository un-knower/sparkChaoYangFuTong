package com.ald.stat.utils

import java.sql.{Connection, DriverManager, SQLException}
import java.util

import com.alibaba.fastjson.JSONException
import org.apache.phoenix.jdbc.PhoenixResultSet

object DBUtils {
  //  lazy val datasource = getDatasource()
  //  lazy Connection
  val batchSize = 1000

  def doExecute(sql: String): Int = {
    use(getConnection) {
      connection => {
        val rint = use(connection.createStatement()) {
          stmt => stmt.executeUpdate(sql)

        }
        rint
      }

    }
  }

  def doBatchExecute(sqls: List[String]): Int = use(getConnection) {
    connection => {
      use(connection.createStatement()) {
        stmt => {
          var count: Int = 0
          sqls.foreach(sql => {
            stmt.addBatch(sql)
            if (count % batchSize == 0) stmt.executeBatch
          })
          count += 1
          stmt.executeBatch
          count
        }
      }
    }
  }

  def getConnection(): Connection = {
    Class.forName(ConfigUtils.getProperty("database.driver"))
    DriverManager getConnection(ConfigUtils.getProperty("database.url"), ConfigUtils.getProperty("database.user"), ConfigUtils.getProperty("database.password"))
  }

  //  def getConnection(): Connection ={
  //    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
  //    DriverManager getConnection("jdbc:phoenix:47.92.119.232:2181")
  //  }
  /**
    * 初始化Driver
    */
  try
    Class.forName(ConfigUtils.getProperty("database.driver"))
  catch {
    case e: ClassNotFoundException => e.printStackTrace()
  }

//  /**
//    * 获取一个Hbase-Phoenix的连接
//    *
//    * @param host
//    * zookeeper的master-host
//    * @param port
//    * zookeeper的master-port
//    * @return
//    */
  //  def getConnection() = {
  //    var cc: Connection = null
  //    val url = "jdbc:phoenix:47.92.119.232:2181"
  //    if (cc == null) try {
  //      // Phoenix DB不支持直接设置连接超时
  //      // 所以这里使用线程池的方式来控制数据库连接超时
  //      val exec = Executors.newFixedThreadPool(1)
  //      val call = new Callable[Connection] {
  //        override def call(): Connection = {
  //          DriverManager.getConnection(url)
  //        }
  //      }
  //      val future = exec.submit(call)
  //      // 如果在5s钟之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞，防止服务夯死
  //      cc = future.get(1000 * 5, TimeUnit.MILLISECONDS)
  //      exec.shutdownNow
  //    } catch {
  //      case e: InterruptedException => e.printStackTrace()
  //    }
  //    cc
  //  }

  /**
    * 根据host、port，以及sql查询hbase中的内容;根据phoenix支持的SQL格式，查询Hbase的数据，并返回json格式的数据
    *
    * @param host
    * zookeeper的master-host
    * @param port
    * zookeeper的master-port
    * @param phoenixSQL
    * sql语句
    * @return json-string
    * @return
    */
  def execSql(host: String, port: String, phoenixSQL: String): String = {
    if (host == null || port == null || (host.trim eq "") || (port.trim eq "")) {
      "必须指定hbase master的IP和端口"
    }
    else if (phoenixSQL == null || (phoenixSQL.trim eq "")) {
      "请指定合法的Phoenix SQL！"
    }
    var result = ""
    try { // 耗时监控：记录一个开始时间
      val startTime = System.currentTimeMillis
      // 获取一个Phoenix DB连接
      val conn = DBUtils.getConnection
      if (conn == null) return "Phoenix DB连接超时！"
      // 准备查询
      val stmt = conn.createStatement
      val set = stmt.executeQuery(phoenixSQL).asInstanceOf[PhoenixResultSet]
      // 查询出来的列是不固定的，所以这里通过遍历的方式获取列名
      val meta = set.getMetaData
      val cols = new util.ArrayList[String]
      while (set.next) {
        if (cols.size == 0) {
          var i = 1
          val count = meta.getColumnCount
          while (i <= count) {
            cols.add(meta.getColumnName(i))
            i += 1;
            i - 1
          }
        }
        result = cols.toString
      }

      // 耗时监控：记录一个结束时间
      val endTime = System.currentTimeMillis
    } catch {
      case e: SQLException => "SQL执行出错：" + e.getMessage
      case e: JSONException => "JSON转换出错：" + e.getMessage
    }
    result
  }

  //  def getDatasource(): DataSource = {
  //
  //    val ds = new DruidDataSource
  //    val prop = new Properties()
  //
  //    ds.setConnectProperties(prop)
  //    ds.setDriverClassName(ConfigUtils.getProperty("database.driver"))
  //       ds.setUsername("")
  //       ds.setPassword("")
  //    ds.setUrl(ConfigUtils.getProperty("database.url"))
  //    ds.setInitialSize(5) // 初始的连接数；
  //    ds.setMaxActive(100)
  //    //   ds.setMinIdle(minIdle)
  //    ds
  //  }

  def use[A <: {def close() : Unit}, B](resource: A)(code: A ⇒ B): B =
    try
      code(resource)
    finally
      resource.close()
}
