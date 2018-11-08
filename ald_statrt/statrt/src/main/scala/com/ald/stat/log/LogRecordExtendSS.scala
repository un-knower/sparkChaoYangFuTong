package com.ald.stat.log

import com.ald.stat.log.LogRecord.logger
import com.alibaba.fastjson.JSON
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer
import com.ald.stat.component.dimension.depthAndDuration.DailyUVDepthDimensionKeyExtend
import org.apache.commons.lang.StringUtils

/**
  * Created by spark01 on 6/6/18.
  */
class LogRecordExtendSS extends LogRecord{

  /**<br>gcs:
    * 事件分析维度下的事件参数.初始值为null 。注意啊，这个ss的含义是时间参数的，
    * 事件参数有两部分组成，key和value，这两部分以":"做分割
    * */
  @BeanProperty
  var ss: String = _

  /**<br>gcs:
    * pd代表page depth。用于忠诚度的访问深度的级别。该类型取值可以取1~6。整型，这里存储为String类型，后期将转成整型
    * */
  @BeanProperty
  var pd: String = _



  /**<br>gcs:<br>dd 代表 dr depth。用于忠诚度的访问时长的级别的指定*/
  @BeanProperty
  var dd: String = _
}

//gcs:==========================================Begin-专用-时间参数-参数明细=================================
object LogRecordExtendSS extends Serializable{

   val logger =LoggerFactory.getLogger("LogRecordExtend")


  def line2Bean(line: String):LogRecordExtendSS ={
    try {
      val logRecordExtend = JSON.parseObject(line, classOf[LogRecordExtendSS])

      //gcs:判断ifo字段，如果ifo字段不为true，此时就把这个ifo字段赋值为false
      if (logRecordExtend != null && !"true".equals(logRecordExtend.ifo)) { //gcs:如果在转换过来的时候发现logRecord的ifo=null，就为它赋值为false。
        logRecordExtend.ifo = "false"
        logRecordExtend
      }
      else {
        logRecordExtend
      }

    }
    catch {
      case ex: Throwable => {
        logger.error(s"convert exeception $line", ex)
        null
      }
    }
  }


  /**<br>gcs:<br>
    * 该函数用于事件分析-时间参数模块，根据ct字段，将logRecord类型的，转换成多个logRecordExtent的类型的数据 <br>
    *   @param logRecord 要将这个LogRecord转换成多个LogRecordExtend的类型的变量
    * */
  def logR2LogRExtendArray(logRecord: LogRecord):ArrayBuffer[LogRecordExtendSS]={

    val delimiter = "\u0001\u0001\u0001" //todo 分隔符有问题
    var logRExtendSSArray =  ArrayBuffer[LogRecordExtendSS]()

    if (StringUtils.isNotBlank(logRecord.ak)&& StringUtils.isNotBlank(logRecord.tp)){ //gcs:如果logRecord当中的ak和tp不为null
      val ak = logRecord.ak
      val tp = logRecord.tp
      var eventParamKey:String = null  //gcs:事件的参数被分为<key,value>两个部分。这个eventParamKey就是时间参数当中的key的部分
      var eventParamValue:String = null
      if (StringUtils.isNotBlank(logRecord.ct)){
        val ct = logRecord.ct
        if (ct.substring(0,1).equals("{")){
          if (!ct.equals("{}")){ //gcs:如果ct字段不为{}
            val ctString = ct.substring(1,ct.length-1)  //gcs:如果要剔除ct字段当中的{}，这里的范围应该是(1,ct.length-2)吧？？？？ ctString是事件参数的集合
             //gcs:"path":"pages/component1/pages/pass4/pass4","chain":"15287686527586479725,15288152248769023032,15288173580348834706"
          val splitComma = ctString.split(",") //gcs:事件参数集合里面的各个事件之间是以逗号进行切分。 有的事件参数的列表可能以（逗号进行分隔）
            for (i <- 0 until splitComma.size){ //gcs:"path":"pages/component1/pages/pass4/pass4" 。事件组：事件参数。一个事件组中有多个参数
              var splitColon = splitComma(i).split(":") //gcs:(path,pages/component1/pages/pass4/pass4)

              try {

                //gcs:若splitColon = "errMsg":"getShareInfo:ok"，这种情况按照":"进行切割，
                // splitColon会被切分成 ("errMsg","getShareInfo,ok")此时就会有错误。
                // 因此，这里还需要再加一层将事件参数的value进行拼接的操作
                if (splitColon.size > 2){
                  for (i <- 2 until splitColon.size){
                    splitColon(1) += ":" +splitColon(i) //gcs:对事件参数的value进行拼接
                  }
                }


                if (splitColon.size >1){ //gcs:pathName:pathValue 的类型。splitColon=事件参数分组:事件参数value
                  if (splitColon(0).contains("\'")){//gcs:判断splitColon包不包含 \  //gcs:splitColon(0) = path
                    if (splitColon(0).length >1){
                      eventParamKey = splitColon(0).substring(1, splitColon(0).length - 1)
                    }
                    else {
                      eventParamKey ="ct"
                    }
                  }
                  else {
                    if (splitColon(0).length >0){
                      eventParamKey = splitColon(0) //path
                    }
                    else {
                      eventParamKey ="ct"
                    }

                  }
                  //gcs:为eventParamValue 进行赋值
                  if (splitColon(1).contains("\'")){ //gcs:如果splitColon(1)当中包含双引号就会把双引号去掉，此时就会将"\"字符去除掉 //gcs:pages/component1/pages/pass4/pass4
                    if (splitColon(1).length >1){
                      eventParamValue = splitColon(1).substring(1, splitColon(1).length - 1)
                    }
                    else {
                      eventParamValue ="null"
                    }
                  }
                  else {
                    if (splitColon(0).length >0){
                      eventParamValue = splitColon(1)
                    }
                    else {
                      eventParamValue ="null"
                    }

                  }

                  var logR = LogRecordExtendSS.logR2logRExtend(logRecord)  //gcs:先将logRecord当中已经赋值的数据转化为logRecordExtend的类型
                  logR.ss = eventParamKey+delimiter+eventParamValue //gcs:为了防止有额外的字符，使用-ald-
                  logRExtendSSArray += logR //gcs:将生成的logR 放到我们的logRExtendSSArray的数组当中
                }
                else if(splitColon.size == 1){  //gcs:这是处理出现了 在一组事件参数的name:value 键值对中只出现了name，然而却没有出现value的情况,例如: 'http':'' 的情况
                  if(splitColon(0).contains("\'")){
                    if (splitColon(0).length >1){

                      var length = splitColon(0).length
                        if (splitColon(0).substring(0,1).equals("\'") ){
                          eventParamKey = splitColon(0).substring(1,length-1)
                        }
                        else if(splitColon(0).substring(length-1,length).equals("\'")){
                          eventParamKey = splitColon(0).substring(0,length -1)
                        }
                      eventParamKey = splitColon(0).substring(1, splitColon(1).length - 1)
                    }
                    else {
                      eventParamKey = "ct"
                    }

                    eventParamValue ="null"
                  }
                  else {
                    if (splitColon(0).length >0){
                      eventParamKey = splitColon(0)
                    }
                    else {
                      eventParamKey = "ct"
                    }

                    eventParamValue ="null"
                  }
                  var logR:LogRecordExtendSS = LogRecordExtendSS.logR2logRExtend(logRecord)
                  logR.ss = eventParamKey+delimiter+eventParamValue
                  logRExtendSSArray += logR
                }
                else {//gcs:splitColon.size ==0
                  //gcs:出现了   事件参数name和事件参数value='':''

                  eventParamKey ="ct"
                  eventParamValue ="null"
                  var logR:LogRecordExtendSS = LogRecordExtendSS.logR2logRExtend(logRecord)
                  logR.ss = eventParamKey+delimiter+eventParamValue

                  logRExtendSSArray += logR
                }

              }catch {

                case  ex:Exception =>
                  null
                case ex:Error =>
                  null
              }



            }
          }
          else { //gcs:此时ct字段等于{}
            var logR:LogRecordExtendSS = LogRecordExtendSS.logR2logRExtend(logRecord)
            logR.ss = s"ct${delimiter}null"  //gcs:在时间参数的<keu,value>部分中，ct = key，value = value_null
            logRExtendSSArray += logR
          }


        }
        else {//gcs:处理不包含"{"左花括号的的ct字段

          val splitComma = logRecord.ct.split(",") //gcs:事件参数集合里面的各个事件之间是以逗号进行切分。 有的事件参数的列表可能以（逗号进行分隔）
          for (i <- 0 until splitComma.size){ //gcs:"path":"pages/component1/pages/pass4/pass4" 。事件组：事件参数。一个事件组中有多个参数
            var splitColon = splitComma(i).split(":") //gcs:(path,pages/component1/pages/pass4/pass4)

            try {

              //gcs:若splitColon = "errMsg":"getShareInfo:ok"，这种情况按照":"进行切割，
              // splitColon会被切分成 ("errMsg","getShareInfo,ok")此时就会有错误。
              // 因此，这里还需要再加一层将事件参数的value进行拼接的操作
              if (splitColon.size > 2){
                for (i <- 2 until splitColon.size){
                  splitColon(1) += ":" +splitColon(i) //gcs:对事件参数的value进行拼接
                }
              }


              if (splitColon.size >1){ //gcs:pathName:pathValue 的类型。splitColon=事件参数分组:事件参数value
                if (splitColon(0).contains("\'")){//gcs:判断splitColon包不包含 \  //gcs:splitColon(0) = path
                  if (splitColon(0).length >1){
                    eventParamKey = splitColon(0).substring(1, splitColon(0).length - 1)
                  }
                  else {
                    eventParamKey ="ct"
                  }
                }
                else {
                  if (splitColon(0).length >0){
                    eventParamKey = splitColon(0) //path
                  }
                  else {
                    eventParamKey ="ct"
                  }

                }
                //gcs:为eventParamValue 进行赋值
                if (splitColon(1).contains("\'")){ //gcs:如果splitColon(1)当中包含双引号就会把双引号去掉，此时就会将"\"字符去除掉 //gcs:pages/component1/pages/pass4/pass4
                  if (splitColon(1).length >1){
                    eventParamValue = splitColon(1).substring(1, splitColon(1).length - 1)
                  }
                  else {
                    eventParamValue ="null"
                  }
                }
                else {
                  if (splitColon(0).length >0){
                    eventParamValue = splitColon(1)
                  }
                  else {
                    eventParamValue ="null"
                  }

                }

                var logR = LogRecordExtendSS.logR2logRExtend(logRecord)  //gcs:先将logRecord当中已经赋值的数据转化为logRecordExtend的类型
                logR.ss = eventParamKey+delimiter+eventParamValue //gcs:为了防止有额外的字符，使用-ald-
                logRExtendSSArray += logR //gcs:将生成的logR 放到我们的logRExtendSSArray的数组当中
              }
              else if(splitColon.size == 1){  //gcs:这是处理出现了 在一组事件参数的name:value 键值对中只出现了name，然而却没有出现value的情况,例如: 'http':'' 的情况


                if(splitColon(0).contains("\'")){
                  if (splitColon(0).length >1){
                    eventParamKey = splitColon(0).substring(1, splitColon(1).length - 1)
                  }
                  else {
                    eventParamKey = "ct"
                  }

                  eventParamValue ="null"
                }
                else {
                  if (splitColon(0).length >0){
                    eventParamKey = splitColon(0)
                  }
                  else {
                    eventParamKey = "ct"
                  }

                  eventParamValue ="null"
                }
                var logR:LogRecordExtendSS = LogRecordExtendSS.logR2logRExtend(logRecord)
                logR.ss = eventParamKey+delimiter+eventParamValue
                logRExtendSSArray += logR
              }
              else {//gcs:splitColon.size ==0
                //gcs:出现了   事件参数name和事件参数value='':''

                eventParamKey ="ct"
                eventParamValue ="null"
                var logR:LogRecordExtendSS = LogRecordExtendSS.logR2logRExtend(logRecord)
                logR.ss = eventParamKey+delimiter+eventParamValue

                logRExtendSSArray += logR
              }

            }catch {

              case  ex:Exception =>
                null
              case ex:Error =>
                null
            }



          }
        }
      }
      else { //gcs:如果ct字段为null时，此时就会在事件参数<key,value>当中，使得key=ct，value=null
        var logR:LogRecordExtendSS = LogRecordExtendSS.logR2logRExtend(logRecord)
        logR.ss = s"ct${delimiter}${logRecord.ct}"  //gcs:ct:value
        logRExtendSSArray += logR
      }
    }


    logRExtendSSArray //gcs:在这种情况下，一个LogRecord会根据ct字段进行拆分，拆分成好多个的LogRecordExtend类型的数据

  }

  /**<br>gcs:<br>
    * 将logRecord转换成LogRecordExtendSS
    * @param logRecord 用来转换成 LogRecordExtendSS 的LogRecord
    * @return 返回一个 LogRecordExtendSS 对象
    * */
  def logR2logRExtend(logRecord: LogRecord):LogRecordExtendSS ={
    var item = JSON.toJSONString(logRecord, true)
    var result = JSON.parseObject(item, classOf[LogRecordExtendSS])
    result
  }






}


object EmojiFilter{

  /**<br>gcs:<br>
    * 用于判断是否是表情符号的正则表达式
    * */
  private final val regex="(?i)[^\\sa-zA-Z0-9\u4E00-\u9FA5)(`~～!--·@#$%^&*()+=|丿\\\\　{}':;',//[//]\\[\\].<>/?~！@#￥%……&*（）——+|{}【】《》_‘；：”“’。，、？]"



  private def isSpecial(str: String): Boolean = {
    var isSpecialChar = false
    for (s <- str) {
      val isMatcher = s.toString.matches(regex)  //gcs:如果这个str的字符串当中的任意的一个字符符合这个正则表达式regex，就认为str当中是包含字符串的
      if (isMatcher) isSpecialChar = true
    }
    isSpecialChar
  }

  /**<br>gcs:<br>
    * 去除表情符号。将所有的表情符号都替换成为 " "
    * */
  def emojiJudgeAndFilter(str: String):String={

    if (isSpecial(str)){
      str.replaceAll(regex, " ")
    }
    else {
      str
    }
  }

}
//gcs:==========================================End-专用-时间参数-参数明细=================================