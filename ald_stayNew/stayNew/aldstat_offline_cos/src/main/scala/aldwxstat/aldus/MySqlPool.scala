package aldwxstat.aldus

import java.sql.{Connection, DriverManager}
import java.util


/**
  * Created by zhangyanpeng on 2017/7/21.
  */
object MySqlPool {
  private val max = 100                                    //连接池连接总数
  private val connectionNum = 10                           //每次产生连接数
  private var conNum = 0                                   //当前连接池已产生的连接数
  private val pool = new util.LinkedList[Connection]()     //连接池
  private val driver = DBConf.driver
  private val url = DBConf.url
  private val username =DBConf.user
  private val password = DBConf.password


  def getJdbcConn() : Connection = {
    //同步代码块  同步锁
    AnyRef.synchronized({
      if(pool.isEmpty){
        //加载驱动
        /*getJdbcConn()*/
        for(i <- 1 to connectionNum){
          //连接数据库
          val conn = DriverManager.getConnection(url,username,password)
          pool.push(conn)
          conNum +=  1
        }
      }
      pool.poll()
    })
  }

  def releaseConn(conn:Connection): Unit ={
    pool.push(conn)
  }

  //加载驱动
  private def preGetConn() : Unit = {
    //控制加载
    if(conNum < max && !pool.isEmpty){
      println("Jdbc Pool has no connection now, please wait a moments!")
      Thread.sleep(2000)
      preGetConn()
    }else{
      Class.forName("com.mysql.jdbc.Driver")
    }
  }
}
