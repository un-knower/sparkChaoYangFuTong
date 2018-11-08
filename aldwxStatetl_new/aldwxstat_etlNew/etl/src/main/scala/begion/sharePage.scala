package begion

import java.sql.Connection

import jdbc.MySqlPool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import aldwxutil.TimeUtil

/**
  * Created by zhangyanpeng on 2017/8/1.
  *
  * 关于页面ETL   将数据保存到 mysql
  */
object sharePage {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 12)
      .appName(this.getClass.getName)
      //.master("local")
      .getOrCreate()
    //创建对象
    val a = new TimeUtil
    val processArgs = a.processArgs2(args)
    println(processArgs)
    //清洗的路径  入库源
    //val path="hdfs://10.56.0.36:9000/ald_log_etl/"+processArgs  //(测试环境)
    val path="hdfs://10.0.100.17:4007/ald_log_etl/"+processArgs    //生产
    //val path="C:\\Users\\zhangyanpeng\\Desktop\\数据\\20171030\\*1.json"

    //读取到的源数据
    val df: DataFrame = spark.read.json(path).filter("ev='page' and ak !=''  and pp != '' and pp !='null'").repartition(100)

    val df_1 =df.select(
        df("ak"),
        df("pp")
      ).distinct()


    var conn:Connection = null
    df_1.foreachPartition((rows:Iterator[Row])=>{
      conn =MySqlPool.getJdbcConn()
      val statement = conn.createStatement
      conn.setAutoCommit(false)
      try {
        conn.setAutoCommit(false)
        rows.foreach(r => {
          val ak = r(0)
          val pp = r(1)
          //ignore
          val sql = s"insert ignore into ald_share_page (app_key,page_uri,page_remark ) values ('${ak}', '${pp}', '${pp}')"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()
      }catch {
        case e: Exception => e.printStackTrace()
          conn.close()
      }
    })

    spark.stop()

  }
}
