package aldwxutils

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Calendar, Date, Locale}

import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.DateFormatUtils


/**
  *  时间工具类
  */

object TimeUtil{
  //获得一天的时间(毫秒)
  val day= 24*3600*1000
  //获得一天时间（秒）
  val dayS= 24*3600

  var flag = false

  //自定义函数计算昨天日期
  def ytime(): String = {
    //创建calendar对象
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val format = new SimpleDateFormat("yyyyMMdd")
    val ydate = cal.getTime
    val newydate = format.format(ydate)
    return newydate
  }

  // 返回今天的日期
  def ytoday(): String = {
    //创建calendar对象
    val cal = Calendar.getInstance()
    val format = new SimpleDateFormat("yyyyMMdd")
    val ydate = cal.getTime
    val newydate = format.format(ydate)
    return newydate
  }

  //自定义函数计算当天日期
  def time():String={
    //创建calendar对象
    val cal = Calendar.getInstance()
    DateFormat.getDateInstance.format(cal.getTime)
  }

  //判断传入的参数是不是日期格式(传参数)

  def processArgs(args: Array[String]):String={
    var date = ""
    for (i <- 0 to args.length-1) {
      if ("-d".equals(args(i))) {
        if (i + 1 < args.length) {
          date = args(i + 1)
        }
      }
    }

    if (StringUtils.isNotBlank(date) && chenkDate(date)){
      val world = date.split("-")
      world(0)+world(1)+world(2)
    }else{
      ytime()
    }
  }

  //判断 日期格式
  def chenkDate(date:String) :Boolean = {
    //定义正则表达式yyyyMMdd
    val regex = "[2][0][0-9]{2}-[0-9]{2}-[0-9]{2}"
    val compile =Pattern.compile(regex)
    //匹配数据
    val matcher =compile.matcher(date)
    matcher.matches()
  }


  //将 时间 转换成 long类型
  def long2int():Long={
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val now = new Date()
    val dt =sdf.parse(sdf.format(now)).getTime-day
    val st =dt+""
    st.substring(0,10).toLong
  }

  //将long 类型转换成 string
  def long2Str(long: Long):String ={
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val data = new Date(long)
    sdf.format(data)
  }


  def getDateString(millis: Long, pattern: String): String = {
    return DateFormatUtils.format(millis,pattern,Locale.ENGLISH)
  }


  def ytimeHive():String={
    //创建calendar对象
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-0)
    val format =new SimpleDateFormat("yyyyMMdd")
    val ydate =cal.getTime
    val newydate = format.format(ydate)
    newydate
  }

  def ytimeHive7():String={
    //创建calendar对象
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-7)
    getDateString(cal.getTime.getTime,"yyyyMMdd")
  }

  def ytimeHive29():String={
    //创建calendar对象
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-30)
    getDateString(cal.getTime.getTime,"yyyyMMdd")
  }

  def str2Long(str :String):Long= {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = sdf.parse(str)
    date.getTime
  }

  //当前时间的 int 类型
  def nowInt():Int={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //创建calendar对象
    val cal = Calendar.getInstance()
    val now = new Date()
    val dt =sdf.parse(sdf.format(now)).getTime
    val st =dt+""
    st.substring(0,10).toInt
  }
  def StrToLong(str :String):Long= {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(str)
    (date.getTime/1000).toLong
  }

  def str2int(string: String):Int={
    val sdf = new SimpleDateFormat("yyyyMMdd")
    //val now = new Date()
    val dt =sdf.parse(sdf.format(string)).getTime
    (dt/1000).toInt
  }

  //字符串转换Timestamp
  def getTimestamp(x:String) :java.sql.Timestamp = {

    val format = new SimpleDateFormat("yyyyMMdd")
    var ts = new Timestamp(System.currentTimeMillis())
    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        val t = new Timestamp(d.getTime())
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }
    return null
  }

  def todayStr():String={
    //创建calendar对象
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    getDateString(cal.getTime.getTime,"yyyyMMdd")
  }

  def processArgs2(args:Array[String]):StringBuffer= {
    var date = ""
    var date2 = ""
    for (i <- 0 to args.length - 1) {
      if ("-d".equals(args(i))) {
        if (i + 2 < args.length) {
          date = args(i + 1)
          date2 = args(i + 2)
        } else if (i + 1 < args.length) {
          date = args(i + 1)
        }
      }
    }
    var str:StringBuffer=new StringBuffer

    if (StringUtils.isNotBlank(date) && chenkDate(date)){
      flag = true
      val world = date.split("-")
      str.append(world(0)+ world(1) + world(2))

      if (StringUtils.isNotBlank(date2)){
        if (chenkDate(date2)){
          val world2 = date2.split("-")
          str.append(","+world2(0) + world2(1)+ world2(2))
        }else{
          str.append(","+ytime())
        }
      }else{
        str.append("")
      }
    }else{
      str.append(ytime())
    }
  }

  //获取 今天的long 类型 时间戳
  def dayLong():Long={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //创建calendar对象
    val cal = Calendar.getInstance()
    val now = new Date()
    val dt =sdf.parse(sdf.format(now)).getTime
    val st =dt+""
    st.toLong
  }

  // 获取上周的周一的日期
  def getoldWeekStart():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    cal.add(Calendar.WEEK_OF_YEAR, -1)
    //获取上周一的日期
    period=df.format(cal.getTime())
    period

  }

  // 获取上周的周日的日期
  def getoldWeekEnd():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);//这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    period=df.format(cal.getTime.getTime)
    period

  }

  //获得 当前的时间（小时数）
  def getHour():String={
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val a = dateFormat.format(now.getTime()-3600000)
    val str =a.split(" ")
    str(1).substring(0,2)
  }

  def getdate():String={
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.format(now.getTime()-3600000)
    val str =a.split("-")
    str(0)+str(1)+str(2)
  }

  // 获取当前时间的时间戳
  def getTimeString(): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val calendar = Calendar.getInstance
    val str = calendar.getTime.getTime+""
    str.substring(0,10)
  }


  //获得传入参数的时间戳
  def processArgs2Long(args: Array[String]) :Long ={
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = processArgs(args)
    val day=date.substring(0,4)
    val hour=date.substring(4,6)
    val min = date.substring(6,8)

    val str = day+"-"+hour+"-"+min
    DateFormat.getDateInstance.parse(str).getTime
    //DateFormat.getDateInstance.parse(date).getTime
  }

  //判断传入的参数是不是日期格式(传参数)
  def processArgsStayTimeDay(args: Array[String]):String={
    var date = ""
    for (i <- 0 to args.length-1){
      if ("-d".equals(args(i))){
        if (i+1 < args.length){
          date =args(i+1)
        }
      }
    }
    if (StringUtils.isNotBlank(date) && chenkDate(date)){
      flag=true
      val world = date.split("-")
      world(0)+world(1)+world(2)
    }else{
      getdate()
    }
  }

  //判断 日期格式
  def chenkhour(date:String) :Boolean = {
    //定义正则表达式yyyyMMdd
    val regex = "[0-9]{2}"
    val compile =Pattern.compile(regex)
    //匹配数据
    val matcher =compile.matcher(date)
    matcher.matches()
  }

  //判断传入的参数是不是日期格式(传参数)  今日日期
  def processArgsStayTimeHour(args: Array[String]):String={
    var hour = ""
    for (i <- 0 to args.length-1){
      if ("-h".equals(args(i))){
        if (i+1 < args.length){
          hour =args(i+1)
        }
      }
    }
    if (StringUtils.isNotBlank(hour) && chenkhour(hour)){
      flag=true
      hour
    }else{
      getHour()
    }
  }


  def main(args: Array[String]): Unit = {
    println(long2int())
    println(processArgs2Long(args))
  }
}
