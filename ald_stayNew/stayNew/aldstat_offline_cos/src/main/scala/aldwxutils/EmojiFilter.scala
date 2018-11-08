package aldwxutils

/**
  * Created by wangtaiyang on 2018/4/8.
  * 过滤和判断表情字符
  */
class EmojiFilter {
  private final val regex="(?i)[^\\sa-zA-Z0-9\u4E00-\u9FA5)(`~～!--·@#$%^&*()+=|丿\\\\　{}':;',//[//]\\[\\].<>/?~！@#￥%……&*（）——+|{}【】《》_‘；：”“’。，、？]"

  //是否包含表情和特殊字符(不算标点符号和空格)
  def isSpecial(str: String): Boolean = {
    var isSpecialChar = false
    for (s <- str) {
      val isMatcher = s.toString.matches(regex)
      if (isMatcher) isSpecialChar = true
    }
    isSpecialChar
  }
  //过滤表情和特殊字符(不算标点符号和空格)
  def filterSpecial(str: String): String = {
    str.replaceAll(regex, " ")
  }


}

object EmojiFilter {
  def main(args: Array[String]): Unit = {
    val ef=new EmojiFilter()
    val b="【合肥~～站】　　cium孟\\京·[辉-经典戏剧作]品《恋爱的犀牛》"
    val bool = ef.filterSpecial(b)
    println(bool)
    println(ef.isSpecial(b))
  }
}

