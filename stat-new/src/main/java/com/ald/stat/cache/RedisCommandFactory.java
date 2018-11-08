package com.ald.stat.cache;

import com.ald.stat.utils.ConfigUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 创建redis的Command连接
 * Created by zhaofw on 2018/7/30.
 */
public class RedisCommandFactory {

    public static RedisCommandConn getInstances(String prefix) {
        String poolInfo = "redis.write.pool";
        if (!StringUtils.isEmpty(prefix)){
            poolInfo = prefix + "." + poolInfo;
        }
        String hostAndProt = ConfigUtils.getProperty(poolInfo);
        String host = hostAndProt.split(":")[0];
        int port = Integer.parseInt(hostAndProt.split(":")[1]);
        return new RedisCommandConn(host,port);
    }

}
