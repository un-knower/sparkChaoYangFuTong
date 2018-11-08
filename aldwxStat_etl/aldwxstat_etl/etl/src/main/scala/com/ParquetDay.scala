package com

import aldwxutil.TimeUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import property.FieldName

/**
  * Created by gaoxiang on 2017/12/11.
  */
object ParquetDay {
  def main(args: Array[String]): Unit = {
    // 设置日志级别为WARN
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.sql.caseSensitive",true)
      .getOrCreate()
    val a = new TimeUtil
    val yesterday =  a.processArgs2(args)
    val conf = new Configuration()
    conf.set("fs.defaultFS", FieldName.hdfsurl)
    val fileSystem = FileSystem.get(conf)
    val paths: String = FieldName.etlpath
    val fs = fileSystem.listStatus(new Path(paths + yesterday))
    val listPath = FileUtil.stat2Paths(fs)

    for (readPath <- listPath) {
      println(readPath)
      val pathnames =readPath.toString.split("/")
      val pathname = pathnames(pathnames.length-1)
      try{
      val df = spark.read.json(s"$readPath")
      df.repartition(1).write.parquet(FieldName.parquetpath + s"$yesterday/$pathname")
      }catch {
        case e: Exception => e.printStackTrace()
        case _:Exception=>println("转parquet出错")
      }
    }
    spark.stop()
  }
}
