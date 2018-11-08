package aldwxutils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Period}
import java.util.Date

/**
  * Created by wangtaiyang on 2017/12/14.
  */
class DateTimeUtil {
  /**
    * 获取相对于今天某天(相聚多少天)日期的指定格式
    *
    * @param day     0 今天，1 明天，-1 昨天
    * @param pattern 日期格式 正则
    * @param year    几年后
    * @param month   几月后
    * @param week    1 表示7天后的日期
    * @return 例如yyyy-MM-dd(指定规则pattern)
    */
  def getUdfDate(day: Int, pattern: String, year: Int = 0, month: Int = 0, week: Int = 0): String = {
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
    LocalDate.now().plusYears(year).plusMonths(month).plusWeeks(week).plusDays(day).format(dtf)
  }

  def long2string(timeStamp: Long, pattern: String): String = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.format(new Date(timeStamp))
  }

  def string2long(date: String, pattern: String): Long = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.parse(date).getTime
  }

  //def intervalDays(first:LocalDate,second:LocalDate,day:Int)


  /**
    * 获取指定日期day，往前numDays天的hdfs目录path所组成的数组
    *
    * @param day     指定日期
    * @param numDays 要获取的天数
    * @return 包含hdfs多个路径的数组
    */
  def getUdfDaysDirs(path: String, day: String, numDays: Int): Array[String] = {
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    //要读取的日志目录数组
    var dirs = Array[String]()
    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt)

    for (i <- -numDays + 1 to 0) {
      val logDir = path + "/" + date.plusDays(i).format(dtf)
      println(s"读取$logDir/*/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*/*")
    }
    dirs
  }

  /**
    * 弹性mr日志目录，获取指定日期往前7天或30天的数据路径的数组
    * cosn://aldwxlogbackup/log_parquet/zzcg-etl-VM-0-224-ubuntu2017120313/
    * cosn://aldwxlogbackup/log_parquet/星2017120313/
    */
  def getTencentDirs(day:String,numDays:Int): Array[String] ={
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt)
    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"
    var dirs = Array[String]()
    for (i <- -numDays + 1 to 0) {
      val logDir =s"$hdfsUrl${date.plusDays(i).format(dtf)}*"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*")
    }
    dirs
  }

  /**
    * 获取erm指定天数的数据集合，可以和上面获取数据的方法整合成一个，后续再改吧
    * @param day 从指定的日期开始算，默认是昨天
    * @param daysArr 数组里面存要获取的天数，-1表示1天前
    * @return 返回要获取的所有天数的emr数据路径的数组
    */
  def getSpecifyTencentDirs(day:String,daysArr:Array[Int]): Array[String] ={
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt)
    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"
    var dirs = Array[String]()
    for (i <- daysArr) {
      val logDir =s"$hdfsUrl${date.plusDays(-i).format(dtf)}*"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*")
    }
    dirs
  }

}

object DateTimeUtil {
  def main(args: Array[String]): Unit = {
    val str = new DateTimeUtil().getUdfDate(0, "yyyyMMdd", 0, 1, 1)
    println(str)
    println("2018-04-10">"2018-04-01")
    import java.time.format.DateTimeFormatter
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val a = LocalDate.parse("20180410",formatter)
    val b = LocalDate.now()
    println(Period.between(a,b).getMonths)
    val day="20180416"
    val date=LocalDate.of(day.substring(0,4).toInt,day.substring(4,6).toInt,day.substring(6,8).toInt)
    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"
    var dirs = Array[String]()
    for (i <- -7 + 1 to 0) {
      val logDir =s"$hdfsUrl${date.plusDays(i).format(formatter)}*"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir)) dirs = dirs.+:(logDir + "/*/*")
    }
  }
}