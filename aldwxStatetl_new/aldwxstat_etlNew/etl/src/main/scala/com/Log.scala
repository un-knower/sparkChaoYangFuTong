package com


import com.alibaba.fastjson.JSONObject

class Log(
           val st                       :Long,
           val day                      :String,
           val hour                     :String,
           val ct                       :String,
           val error_messsage           :String,
           val ct_path                  :String,
           val ct_chain                 :String,
           val dr                       :Long,
           val ag                       :String,
           val ag_ald_link_key          :String,
           val ag_ald_position_id       :String,
           val ag_ald_media_id          :String,
           val et                       :String,
           val lp                       :String,
           val ev                       :String,
           val ak                       :String,
           val ifo                      :String,
           val uu                       :String,
           val at                       :String,
           val pp                       :String,
           val path                     :String,
           val ifp                      :String,
           val province                 :String,
           val tp                       :String,
           val qr                       :String,
           val client_ip                :String,
           val scene                    :String,
           val city                     :String,
           val nt                       :String,
           val ag_aldsrc                :String,
           val wsr_query_aldsrc         :String,
           val wsr_query_ald_share_src  :String,
           val lang                     :String,
           val wv                       :String,
           val wsdk                     :String,
           val pm                       :String,
           val v                        :String,
           val wh                       :String,
           val ww                       :String,
           val wvv                      :String,
           val sv                       :String,
           val nickName                 :String,
           val gender                   :String,
           val language                 :String,
           val country                  :String,
           val avatarUrl                :String
         ) extends Product with Serializable{
  override def productElement(n: Int): Any = n match {
    case 0   =>st
    case 1   =>day
    case 2   =>hour
    case 3   =>ct
    case 4   =>error_messsage
    case 5   =>ct_path
    case 6   =>ct_chain
    case 7   =>dr
    case 8   =>ag
    case 9   =>ag_ald_link_key
    case 10  =>ag_ald_position_id
    case 11  =>ag_ald_media_id
    case 12  =>et
    case 13  =>lp
    case 14  =>ev
    case 15  =>ak
    case 16  =>ifo
    case 17  =>uu
    case 18  =>at
    case 19  =>pp
    case 20  =>path
    case 21  =>ifp
    case 22  =>province
    case 23  =>tp
    case 24  =>qr
    case 25  =>client_ip
    case 26  =>scene
    case 27  =>city
    case 28  =>nt
    case 29  =>ag_aldsrc
    case 30  =>wsr_query_aldsrc
    case 31  =>wsr_query_ald_share_src
    case 32  =>lang
    case 33  =>wv
    case 34  =>wsdk
    case 35  =>pm
    case 36  =>v
    case 37  =>wh
    case 38  =>ww
    case 39  =>wvv
    case 40  =>sv
    case 41  =>nickName
    case 42  =>gender
    case 43  =>language
    case 44  =>country
    case 45  =>avatarUrl
  }

  override def productArity: Int = 46
//  override def productArity: Int = 45

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Log]
}
object Log{
  def apply(jSONObject: JSONObject): Log ={
    //    jSONObject.getString("")
    new Log(
      if (jSONObject.getLong("st")==null) 0l else jSONObject.getLong("st"),
      s"${jSONObject.getString("day")}",
      s"${jSONObject.getString("hour")}",
      s"${jSONObject.getString("ct")}",
      s"${jSONObject.getString("error_messsage")}",
      s"${jSONObject.getString("ct_path")}",
      s"${jSONObject.getString("ct_chain")}",
      if(jSONObject.getLong("dr")==null)0l else jSONObject.getLong("dr"),
      s"${jSONObject.getString("ag")}",
      s"${jSONObject.getString("ag_ald_link_key")}",
      s"${jSONObject.getString("ag_ald_position_id")}",
      s"${jSONObject.getString("ag_ald_media_id")}",
      s"${jSONObject.getString("et")}",
      s"${jSONObject.getString("lp")}",
      s"${jSONObject.getString("ev")}",
      s"${jSONObject.getString("ak")}",
      s"${jSONObject.getString("ifo")}",
      s"${jSONObject.getString("uu")}",
      s"${jSONObject.getString("at")}",
      s"${jSONObject.getString("pp")}",
      s"${jSONObject.getString("path")}",
      s"${jSONObject.getString("ifp")}",
      s"${jSONObject.getString("province")}",
      s"${jSONObject.getString("tp")}",
      s"${jSONObject.getString("qr")}",
      s"${jSONObject.getString("client_ip")}",
      s"${jSONObject.getString("scene")}",
      s"${jSONObject.getString("city")}",
      s"${jSONObject.getString("nt")}",
      s"${jSONObject.getString("ag_aldsrc")}",
      s"${jSONObject.getString("wsr_query_aldsrc")}",
      s"${jSONObject.getString("wsr_query_ald_share_src")}",
      s"${jSONObject.getString("lang")}",
      s"${jSONObject.getString("wv")}",
      s"${jSONObject.getString("wsdk")}",
      s"${jSONObject.getString("pm")}",
      s"${jSONObject.getString("v")}",
      s"${jSONObject.getString("wh")}",
      s"${jSONObject.getString("ww")}",
      s"${jSONObject.getString("wvv")}",
      s"${jSONObject.getString("sv")}",
      s"${jSONObject.getString("nickName")}",
      s"${jSONObject.getString("gender")}",
      s"${jSONObject.getString("language")}",
      s"${jSONObject.getString("country")}",
      s"${jSONObject.getString("avatarUrl")}"

    )
  }
}
