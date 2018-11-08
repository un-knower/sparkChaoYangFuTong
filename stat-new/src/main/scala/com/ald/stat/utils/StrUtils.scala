package com.ald.stat.utils

/**
  * Created by zhaofw on 2018/8/15.
  */
object StrUtils {
  /**
    * 判断一个字符串是否是正整数
    * @param str
    * @return
    */
  def isInt(str:String):Boolean ={
    import java.util.regex.Pattern
    val isInt: Boolean = Pattern.compile("""^\d+$""").matcher(str).find
    isInt
  }

}
