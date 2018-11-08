package com.ald.stat.log

//import org.apache.spark.streaming.dstream.DStream
//import services.WeatherTools

import scala.beans.BeanProperty
import com.alibaba.fastjson.{JSON, JSONArray}

/**
  * Created by eversec on 2017/6/24.
  */
class EventParamsBean() extends Serializable {
  //eventType等于1,2,86,87的字段信息
  @BeanProperty
  var n: String = "defalt"
  @BeanProperty
  var u: String = "defalt"
  @BeanProperty
  var t: String = "defalt"
  @BeanProperty
  var y: String = "defalt"
  @BeanProperty
  var x: String = "defalt"
  @BeanProperty
  var eventType: String = "defalt"
  @BeanProperty
  var id: String = "defalt"
  @BeanProperty
  var netAll: String = "defalt"
  @BeanProperty
  var netDns: String = "defalt"
  @BeanProperty
  var netTcp: String = "defalt"
  @BeanProperty
  var srv: String = "defalt"
  @BeanProperty
  var dom: String = "defalt"
  @BeanProperty
  var loadEvent: String = "defalt"
  @BeanProperty
  var qid: String = "defalt"

  @BeanProperty
  var category:String = "default"
  @BeanProperty
  var action:String = "default"
  @BeanProperty
  var label:String = "default"
  @BeanProperty
  var value:String = "default"
}
object EventParamsBean {

//  /**
//    *
//    * @param eventParams
//    * @return
//    */
//  /* def EventParamsJsonBean(eventParams:String): EventParamsBean = {
//    val jsonDS = WeatherTools.FilterDStream(eventParams)
//     val jsonObject1 = JSONObject.fromObject(eventParams)
//     JSONObject.toBean(jsonObject1, classOf[EventParamsBean]).asInstanceOf[EventParamsBean]
//  }*/

//  def EventParamsJsonBean4ev3(logsBean:LogRecord): EventParamsBean = {
//    val params: String = logsBean.getEventParams
//    if(!"default".equals(params)){
//      val parseArray: JSONArray = JSON.parseArray(params)
//      val eventparamsBean: EventParamsBean = JSON.parseObject(parseArray.get(0).toString, classOf[EventParamsBean])
//      //   println(eventparamsBean.getVt)
//      eventparamsBean
//    }else{
//      val eb = new EventParamsBean
//      eb
//    }
//  }

}
