package aldwxutils

import java.sql.{Connection, DriverManager, SQLException}
import java.util

import aldwxconfig.{ConfigurationUtil, Constants}
import com.mysql.jdbc.MysqlDataTruncation
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer


/**
  * 数据库配置, 批量入库
  */
object Jdbc_utf8mb4 {
  private var pools: util.LinkedList[Connection] = _ //连接池
  private val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER)
  private val url = ConfigurationUtil.getProperty(Constants.JDBC_URL)
  private val username = ConfigurationUtil.getProperty(Constants.JDBC_USER)
  private val password = ConfigurationUtil.getProperty(Constants.JDBC_PWD)

  /**
    * 加载驱动
    */
  Class.forName(driver)

  /**
    * 从数据库连接池中获取连接对象
    *
    * @return
    */

  def getConn(): Connection = {

    synchronized({
      try {
        if (pools == null) {
          pools = new util.LinkedList[Connection]()
          //连接池大小=cup数*2+磁盘数
          for (_ <- 0 until 17) {
            val connection = DriverManager.getConnection(url, username, password)

            pools.push(connection)
          }
        }

      } catch {
        case e: SQLException => e.printStackTrace()
      }
      /*这里如果不加判断，可能会发生取出null值的情况，
      但是实际任务中只出现过一两次，故先不做判断，继续观察任务情况*/
      //      if (pools.size() == 0) {
      //        do {
      //          println("线程池空了，睡3s")
      //          Thread.sleep(3000)
      //        } while (pools.size() == 0)
      //      }
      return pools.poll()
    })
  }

  /**
    * 当连接对象用完后，需要调用这个方法归还连接对象到连接池中
    *
    * @param connection
    */
  def returnConnection(connection: Connection): Unit = {
    pools.push(connection)
  }

  /**
    * 数据库批量入库
    *
    * @param sqlText String 类型, sql 语句, insert into table_name ... ON DUPLICATE KEY UPDATE ...
    * @param params  Array 类型, sql 数据
    */
  def doBatch(sqlText: String, params: ArrayBuffer[Array[Any]]): Unit = {
    var conn = getConn()
    if (conn == null) {
      do {
        println("线程池空了，睡3s")
        Thread.sleep(3000)
        conn = getConn()
      } while (conn == null)
    }
    //设置编码
    val set_utf8mb4=conn.prepareStatement("set names utf8mb4")
    set_utf8mb4.execute()
    if (params.nonEmpty) { // 判断sql 数据数组是否为空
      val pstat = conn.prepareStatement(sqlText) // 预编译 sql 语句
      pstat.clearBatch() // 清除批处理
      var count = 0
      for (param <- params) {
        for (index <- param.indices) {
          pstat.setObject(index + 1, param(index)) // index + 1 是指 sql 语句的第几个参数, param(index)是要设置的值
        }
        pstat.addBatch() // 添加到批次中
        count += 1
        if (count == 1000) {
          try {
            pstat.executeBatch() // 批量执行
          } catch {
            case e: SQLException =>
              println(s"插入mysql异常,状态码：${e.getSQLState}，错误码：${e.getErrorCode},message:${e.getMessage},错误的数据${param.mkString(",")}")
            case e: MysqlDataTruncation =>
              println(s"插入异常==MysqlDataTruncation ：${e.getMessage}，异常数据：${param.mkString(",")}")
            case e: Exception =>
              println(s"插入异常：${e.getMessage}，异常数据：${param.mkString(",")}")
          }
          count = 0
        }
      }
      try{
        pstat.executeBatch() // 批量执行
      }catch {
        case e: SQLException =>
          println(s"插入mysql异常,状态码：${e.getSQLState}，错误码：${e.getErrorCode},message:${e.getMessage}")
        case e: MysqlDataTruncation =>
          println(s"插入异常==MysqlDataTruncation ：${e.getMessage}")
        case e: Exception =>
          println(s"插入异常：${e.getMessage}")
      }

      params.clear() // 清除 params 数组
    }else{
      println("传过来的字段参数params是空的")
    }
    returnConnection(conn)
  }

  //批量并且失败回滚
  /* def doBatch(sqlText: String, params: ArrayBuffer[Array[Any]]): Unit = {
     var conn = getConn()
     //不判断的话可能取到null值，因为是并行去list中取的
     while (conn == null) {
       println("doBatch中的conn为空了,等待3秒在重试..")
       Thread.sleep(3000)
       conn = getConn()
     }
     val pstat = conn.prepareStatement(sqlText) // 预编译 sql 语句
     try {
       conn.setAutoCommit(false)
       if (params.nonEmpty) { // 判断sql 数据数组是否为空
         var count = 0
         pstat.clearBatch() // 清除批处理
         for (param <- params) {
           for (index <- param.indices) {
             // index + 1 是指 sql 语句的第几个参数, param(index)是要设置的值
             pstat.setObject(index + 1, param(index))
           }
           pstat.addBatch() // 添加到批次中
           count += 1
           if (count == 1000) {
             pstat.executeBatch() // 批量执行
             conn.commit()
             count = 0
           }
         }
         pstat.executeBatch() // 批量执行
         conn.commit()
         println(s"提交后conn为 $conn")
         params.clear() // 清除 params 数组
       }
     } catch {
       case e: SQLException =>
         println(s"插入mysqls异常,状态码：${e.getSQLState}，错误码：${e.getErrorCode},message:${e.getMessage}")
         println(e.printStackTrace())
         try {
           println("插入异常，sql回滚")
           println(s"rollback前conn为 $conn")
           conn.rollback()
           println(s"rollback后conn为 $conn")
         } catch {
           case er: SQLException =>
             println(er.printStackTrace())
         }
     } finally {
       close(pstat, conn)
     }


   }*/

  def readFromMysql(sparkSession: SparkSession, table: String): DataFrame = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_URL)
    val user = ConfigurationUtil.getProperty(Constants.JDBC_USER)
    val password = ConfigurationUtil.getProperty(Constants.JDBC_PWD)
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER)
    // 从 mysql 中读取数据
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
    jdbcDF
  }
}
