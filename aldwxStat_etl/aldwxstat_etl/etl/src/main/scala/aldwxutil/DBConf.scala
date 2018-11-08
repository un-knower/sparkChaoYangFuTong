package aldwxutil

/**
  * Created by zhangyanpeng on 2017/8/2.
  */

/**
* <b>author:</b> gcs <br>
* <b>data:</b> 18-7-9 <br>
* <b>description:</b><br>zzzzzz
  *   在这个文件中可以指定我们做测试的时候使用线上的数据库还是测试数据库。
  *   在使用测试数据库的时候，首先需要执行程序往测试数据库中写入数据。之后才可以
* <b>param:</b><br>
* <b>return:</b><br>
*/
object DBConf {
  val debug=1 //gcs:当debug等于1的时候就是使用线上的MySql数据。当debug等于0的时候就是使用测试的MySql数据库
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
