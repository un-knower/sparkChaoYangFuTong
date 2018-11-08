package aldwxstat

import aldwxutils.{Jdbc_utf8mb4, JudgeVersion, TimeUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * update by youle on 2018/8/2
  */
object PictureUrl {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()
    //获取天数据
    val yesterday = TimeUtil.processArgs(args)
    var old_url_path=s"hdfs://10.0.100.17:4007/aldwx/uhtml/${yesterday}/*"//老版SDK数据路径
    var new_url_path=s"hdfs://10.0.100.17:4007/ald_jsonlogs/${yesterday}/*.json"//新版SDK数据路径
    //如果参数中指定小时，则更改路径
    if(args.length==4){
      val hour=args(3)
      old_url_path=s"hdfs://10.0.100.17:4007/aldwx/uhtml/${yesterday}/*${yesterday}${hour}*"
      new_url_path=s"hdfs://10.0.100.17:4007/ald_jsonlogs/${yesterday}/*${yesterday}${hour}*.json"
    }
    println("获取老版SDK头像路径："+old_url_path)
    println("获取新版SDK头像路径："+new_url_path)

    //获取老版SDK数据
    val value: RDD[Row] = sparkSession.sparkContext.textFile(old_url_path).map(line => {
      val jsonLine: JSONObject = JSON.parseObject(line)
      val uu = jsonLine.get("uu")
      val nickName = jsonLine.get("nickname")
      val avatarUrl = jsonLine.get("avatarurl")
      Row(uu,nickName,avatarUrl)
    })
    val schema = StructType(List(
      StructField("uu", StringType, true),
      StructField("nickName", StringType, true),
      StructField("avatarUrl", StringType, true)
    )
    )
    val df_tmp=sparkSession.createDataFrame(value,schema)
    df_tmp.createTempView("df_tmp")
    val df=sparkSession.sql("select uu, nickName,avatarUrl from df_tmp where uu !='null' and nickName !='null' and nickName !=''").distinct()
    //插入老版SDK头像数据
    df.foreachPartition(line=>{
      dataframe2mysql(line)
    })

    //处理新版SDK头像数据
    val value2: RDD[Row] = sparkSession.sparkContext.textFile(new_url_path).map(line => {
      var uu=""
      var nickName = ""
      var avatarUrl = ""
      try {
        //有部分json数据异常，执行JSON.parseObject(line)会报错，需要加try，catch
      if(JSON.parseObject(line).get("uu")!=null){
        val jsonLine: JSONObject = JSON.parseObject(line)
        uu = jsonLine.get("uu").toString
        val version= jsonLine.get("v")
        //新版
        if(version !=null){
          if (JudgeVersion.version(version)) {
            val ufo = jsonLine.get("ufo")
            if(ufo != null){
              val userInfo = JSON.parseObject(ufo.toString).get("userInfo").toString
              if(JSON.parseObject(userInfo).get("nickName")!=null){
                nickName = JSON.parseObject(userInfo).get("nickName").toString
              }
              if(JSON.parseObject(userInfo).get("avatarUrl")!=null){
                avatarUrl = JSON.parseObject(userInfo).get("avatarUrl").toString
              }
            }
          }
        }
      }
      }catch {
        case ex: Throwable =>println("found a unknown exception"+ ex+line)
      }
      Row(uu,nickName,avatarUrl)
    })
    val schema2 = StructType(List(
      StructField("uu", StringType, true),
      StructField("nickName", StringType, true),
      StructField("avatarUrl", StringType, true)
    )
    )
    val df_tmp2=sparkSession.createDataFrame(value2,schema2)
    df_tmp2.createTempView("df_tmp2")
    val df2=sparkSession.sql("select uu, nickName,avatarUrl from df_tmp2 where uu !='' and nickName !=''").distinct()

    //插入新版SDK头像数据
    df2.foreachPartition(line=>{
      dataframe2mysql(line)
    })
  }
  def dataframe2mysql(iterator: Iterator[Row]): Unit = {
    val updateTime = System.currentTimeMillis()/1000
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into ald_wechat_user_bind (uuid,openid,unionid,nickname,country,province,city,gender,avatar_url,user_remark,create_at) values (?,?,?,?,?,?,?,?,?,?,?)" +
      s" ON DUPLICATE KEY UPDATE nickname=?,avatar_url=?,create_at=?"
    iterator.foreach(r => {
      val openid=0
      val unionid=0
      val country = ""
      val province = ""
      val city=""
      val user_remark=""
      val gender=0
      val uu = r(0)
      val nickname = r(1)
      val avatarUrl = r(2)
      val create_at=updateTime
      params.+=(Array[Any](uu,openid,unionid,nickname,country,province,city,gender,avatarUrl,user_remark,create_at,nickname,avatarUrl,create_at))
    })
    //nickname要采用utf8mb4编码
    Jdbc_utf8mb4.doBatch(sqlText, params)
  }
}
