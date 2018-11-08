package stat.commos;

import java.io.Serializable;

/**
 * Created by zhangyanpeng on 2017/7/26.
 * <p>
 * 2017 11 03 调整字段 v
 */
public class FilePaths implements Serializable {


    /**
     * 腾讯云
     */
    //hdfs 的本地url
    public static String hdfsurl = "hdfs://10.0.0.212:9000";
//    public static String hdfsurl = "hdfs://10.0.0.58:4007";
    //文件路径读取
    public static String jsonpath = "hdfs://10.0.0.212:9000/ald_jsonlogs/";
//    public static String jsonpath = "hdfs://10.0.0.58:4007/ald_jsonlogs/";
    public static String etl_result = "hdfs://10.0.0.58:4007/etl_result/";
    public static String parquet_result = "hdfs://10.0.0.58:4007/parquet_result/";
}
