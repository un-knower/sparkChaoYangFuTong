package property;


import java.io.Serializable;

/**
 * Created by zhangyanpeng on 2017/7/26.
 * <p>
 * 2017 11 03 调整字段 v <br>
 * 这个字段是什么含义啊???
 */
public class FieldName implements Serializable {

    //需要特殊处理的字段不需要在这里添加
    public static String ff = "et,lp,ev,ak,ifo,uu,at,pp,path,ifp,province,tp,qr,client_ip,scene,city,nt,ag_aldsrc,wsr_query_aldsrc,wsr_query_ald_share_src,lang,wv,wsdk,pm,v,wh,ww,wvv,sv";


    /**
     * 腾讯云
     */
    //hdfs 的本地url
    public static String hdfsurl = "hdfs://10.0.100.17:4007";

    /**<br>gcs:<br>这是json文件的存储的路径
     * 经过Flume处理完成的原始ETl数据
     * 经过Flume处理完的原始的ETl数据，存储的路径是 hdfs://10.0.100.17:4007/ald_jsonlogs/$today
     *
     * */
    public static String jsonpath = "hdfs://10.0.100.17:4007/ald_jsonlogs/";
    public static String parquet = "hdfs://10.0.100.17:4007/ald_log_parquet/";

    /**<br>gcs:<br>
     * 经过第一次ETL处理，即转化为parquet数据之前的etl数据存储在这个路径下面。
     * 经过第一次ETL处理完的原始的数据，存储的路径是 hdfs://10.0.100.17:4007/ald_log_etl/$today/etl-$today$hour
     * */
    public static String etlpath="hdfs://10.0.100.17:4007/ald_log_etl/";

    /**<br>gcs:<br>
     * 经过第一次ETL处理转化为parquet类型数据，都存储在这个路径下面。
     * 经过第一次ETL处理完的原始的数据，存储的路径是 hdfs://10.0.100.17:4007/ald_log_parquet/$today/etl-$today$hour
     * */
    public static String parquetpath= "hdfs://10.0.100.17:4007/ald_log_parquet/";

    /**<br>gcs:<br>
     * 经过第一次错误分析模块专用的数据的ETL处理，即转化为parquet数据之前的etl数据和转化为parquet类型数据，都存储在这个路径下面。
     * 经过第一次ETL处理完的原始的数据，存储的路径是 hdfs://10.0.100.17:4007/ald_errlogs/$today/$pathName
     * */
    public static String errpath = "hdfs://10.0.100.17:4007/ald_errlogs/";

    /**<br>gcs:<br>
     * 经过第一次错误分析模块专用的数据的ETL处理，即转化为parquet数据之前的etl数据和转化为parquet类型数据，都存储在这个路径下面。
     * 经过第一次ETL处理完的原始的数据，存储的路径是 hdfs://10.0.100.17:4007/ald_errlogs/$today/etl-$today$hour
     * */
    public static String errparquet = "hdfs://10.0.100.17:4007/ald_err_parquet/";


    /*
    *gcs:
    *最新版本的代码的配置信息
    //hdfs 的本地url
    public static String hdfsurl = "hdfs://10.0.100.17:4007";
    //文件路径读取
    public static String jsonpath = "hdfs://10.0.100.17:4007/ald_jsonlogs/";
    public static String parquet = "hdfs://10.0.100.17:4007/ald_log_parquet/";
    //统计etl json 和parquet 文件路径存放
    public static String etlpath="hdfs://10.0.100.17:4007/ald_log_etl/";
    public static String parquetpath= "hdfs://10.0.100.17:4007/ald_log_parquet/";

    //错误日志 etl 后json和parquet文件路径存放
    public static String errpath = "hdfs://10.0.100.17:4007/ald_errlogs/";
    public static String errparquet = "hdfs://10.0.100.17:4007/ald_err_parquet/";
    */



    /**
     * 测试etl的存放路径
     */
    //    public static String etlpath = "hdfs://10.0.100.17:4007/test/etl/";
    //    public static String parquetpath = "hdfs://10.0.100.17:4007/test/parquet/";
    //    public static String errpath = "hdfs://10.0.100.17:4007/test/err/";
    //    public static String errparquet = "hdfs://10.0.100.17:4007/test/errparquet/";

    public static String[] getName() {
        String[] fields = ff.split(",");
        return fields;
    }
}
