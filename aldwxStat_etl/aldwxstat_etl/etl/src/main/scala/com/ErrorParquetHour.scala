package com

import aldwxutil.TimeUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import property.FieldName

/**
  * create by gaoxiang 2018-01-06
  * Scala code is to convert ERRORETL JSON data into ERRORparquet data
  */
object ErrorParquetHour {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.speculation","true")
      .getOrCreate()
    val aldTimeUtil = new TimeUtil
    //获取今天时间
    val today =  aldTimeUtil.processArgs(args)
    //获取当前小时的前一个小时
    val hour: String = aldTimeUtil.processArgsHour(args)
    //今天的日期加上小时
    val  th= today+hour
    val conf = new Configuration()
    conf.set("fs.defaultFS",  FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    val paths: String = FieldName.errpath
    //读取到今天的 erroretl-json路径下的所有文件
    val fs = fileSystem.listStatus(new Path(paths + today))
    val listPath = FileUtil.stat2Paths(fs)
    for (d <- listPath) {
      //for循环处理判读如果包含今天上一个小时的问题件夹则进行处理
      if (d.toString.contains(s"$th")){
        //对文件路径进行切分 目的是为了存数据的文件夹名称
        val pathnames =d.toString.split("/")
        //取切分后的最后一个为名称
        val pathname = pathnames(pathnames.length-1)
        try{
        val df = spark.read.json(s"$d").repartition(100)
        df.repartition(1).write.parquet(FieldName.errparquet +s"$today/$pathname")
        }catch {
          case e: Exception => e.printStackTrace()
          case _:Exception=>println("转parquet出错:"+d.toString)
        }
        }
    }
    spark.stop()
  }
}
