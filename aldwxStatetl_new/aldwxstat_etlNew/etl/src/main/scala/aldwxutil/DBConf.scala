package aldwxutil

/**
  * Created by zhangyanpeng on 2017/8/2.
  */
object DBConf {
  val debug=1
  val driver="com.mysql.jdbc.Driver"

  //本地
  /*val url= "jdbc:mysql://localhost/ald?useUnicode=true&characterEncoding=UTF-8"
  val user="root"
  val password="root"*/

  //测试集群 2
  /*val url= "jdbc:mysql://10.56.0.36:3306/ald_xinen?useUnicode=true&characterEncoding=UTF-8"
  val user="aldwx"
  val password="aldwx123"*/


  //腾讯云
  /*val url= "jdbc:mysql://10.0.0.61:3306/ald_xinen?useUnicode=true&characterEncoding=UTF-8"
  val user="aldwx"
  val password="wxAld2016__$#"*/

  var url      = ""
  var user     = ""
  var password = ""
  if (debug==0) {
    url = "jdbc:mysql://10.0.0.61:3306/test?useUnicode=true&characterEncoding=UTF-8"
    user = "aldwx"
    password = "wxAld2016__$#"
  } else {
    url= "jdbc:mysql://10.0.0.179:3306/ald_xinen?useUnicode=true&characterEncoding=UTF-8"
    user="cdb_outerroot"
    password="%06Ac9c@317Hb&"
  }

  /*val url= "jdbc:mysql://10.0.0.61:3306/TestEtl?useUnicode=true&characterEncoding=UTF-8"
    val user="aldwx"
    val password="wxAld2016__$#"*/

}
