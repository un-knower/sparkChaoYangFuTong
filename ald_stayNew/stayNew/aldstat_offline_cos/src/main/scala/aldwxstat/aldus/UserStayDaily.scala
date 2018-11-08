package aldwxstat.aldus

//==========================================================
/*
*gcs:
*新的用户留存的代码
* -d 是补昨天的数据
* 如果补今天的7天和30天的数据。那么运行这个程序就什么参数也不用添加。
* 如果要补前天(2018-06-30)的7天和30天的数据。那么-d 后面添加的参数是"-d 2018-06-29"。
* 因为7天和30天的数据是默认是从昨天开始补的数据
*/

import java.sql.Statement
import java.text.SimpleDateFormat
import java.util.Date

import aldwxstat.aldus.MySqlPool
import aldwxutils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object UserStayDaily {
  def main(args: Array[String]) {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2018-06-21", "zhangshijian")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      //        .master("local[*]")
      .getOrCreate()

    //获得昨天时间
    val tt = TimeUtil.processArgs(args)
    //获得昨天时间戳
    val nowtime = TimeUtil.str2Long(tt)
    //获得传入参数的日期 +1 天

    //将long 类型转换成 string
    def long2Str(long: Long): String = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val data = new Date(long)
      sdf.format(data)
    }

    //==========================================================1
    /*
    *gcs:
    *创建一个时间数组。将1-7,8,15,30天，这些时间存放到timeArg这个数组当中
    */
    val timeArg = new collection.mutable.ArrayBuffer[String]()

    //==========================================================2
    /*
    *gcs:
    *以此遍历这个时间数组
    */
    for (i <- 0 to 7) {
      timeArg.append(long2Str(nowtime - 86400 * 1000 * i.toLong))
    }
    timeArg.append(long2Str(nowtime - 86400 * 1000 * 14.toLong))
    timeArg.append(long2Str(nowtime - 86400 * 1000 * 30.toLong))


//    logger.debug(s"\nread the following day directory:\n${timeArg.mkString("\n")}")


    val df0 = ArgsTool.getSpecifyTencentDateDF(args, spark, Array(0))
    //    val df0: DataFrame = spark.read.parquet(s"D:\\Program\\data\\${timeArg(0)}")
    val todayDf = df0.select(
      df0("ak"),
      df0("uu")
    )
      .filter(s"ev='app'")
    todayDf.cache()

    //    for (i <- 4 to 9) {
    for (i <- 0 to 9) {
      val df = ArgsTool.getSpecifyTencentDateDF(args, spark, Array(i))
      val otherDf = df.select(
        df("ak").alias("base_ak"),
        df("uu").alias("base_uu"),
        expr("case when ifo='true' then 1 else 0 end").alias("base_is_first_come")
      )
        .filter(s"ev='app'")

      //计算当天uv,计算新增uv
      val uvDf = otherDf.groupBy("base_ak")
        .agg(countDistinct("base_uu").alias("active_uv"), countDistinct(expr("case when base_is_first_come=1 then base_uu else null end")).alias("new_uv"))
        .select("base_ak", "active_uv", "new_uv")

      val active_leftDf = todayDf.join(otherDf, otherDf("base_ak") === todayDf("ak") && otherDf("base_uu") === todayDf("uu"))
        .select("base_ak", "base_uu", "base_is_first_come")

      //留存人数,新增留存人数
      val people_left = active_leftDf.groupBy("base_ak")
        .agg(countDistinct("base_uu").alias("active_left"), countDistinct(expr("case when base_is_first_come=1 then base_uu else null end")).alias("new_left"))
        .select("base_ak", "active_left", "new_left")
        .withColumnRenamed("base_ak", "ak")

      //关联
      val result = uvDf.join(people_left, uvDf("base_ak") === people_left("ak"), "left_outer")
        .select("base_ak", "active_uv", "new_uv", "active_left", "new_left")
        .na.fill(0)

      result.createOrReplaceTempView("v_tmp")

      val day = if (i == 8) 14 else if (i == 9) 30 else i
      val rs = spark.sql(
        s"""
           |select base_ak,'${timeArg(i)}' day,$day as day_after,active_left,new_left,
           |case when active_uv=0 then 0 else cast(active_left/active_uv as float) end active_people_ratio,
           |case when new_uv=0 then 0 else cast(new_left/new_uv as float)  end  as new_people_ratio
           |from v_tmp
         """.stripMargin)


      // 写入数据库
      insert2db(rs)
    }

    spark.stop()
  }

  private def insert2db(rs: DataFrame) = {
    rs.foreachPartition((rows: Iterator[Row]) => {
      //连接
      var statement: Statement = null
      val conn = MySqlPool.getJdbcConn()
      statement = conn.createStatement()
      try {
        conn.setAutoCommit(false)
        rows.foreach(r => {
          val ak = r(0)
          val day = r(1)
          val diff = r(2)
          val people_left = r(3) //活跃用户
          val new_people_left = r(4) //新增用户
          val active_people_ratio = r(5) //活跃用户留存比
          val new_people_ratio = r(6) //新增用户留存比

          val sql = s"insert into ald_stay_logs (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio)" +
            s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}') ON DUPLICATE KEY UPDATE new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}'"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (conn != null)
          conn.close()
      }
    })
  }
}
