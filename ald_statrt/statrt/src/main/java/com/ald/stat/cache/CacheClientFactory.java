package com.ald.stat.cache;


import com.ald.stat.cache.impl.JedisClientPool;
import com.ald.stat.cache.impl.RedisCacheImpl;
import com.ald.stat.cache.impl.RedisClusterImpl;
import com.ald.stat.utils.ConfigUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * User: liyj12
 * Date: 2015/2/6
 * Time: 16:00
 */
public class CacheClientFactory {

    static RedisCache redisCache;

    /**
    * <b>author:</b> gcs <br>
    * <b>data:</b> 18-7-10 <br>
    * <b>description:</b><br>
     *     获得一个Redis的线程池
    * <b>param:</b><br>
    * <b>return:</b><br>
    */
    public static RedisCache getInstances() {
        //==========================================================1
        /*
        *gcs:
        *这个ConfigUtils.getProperty("client.redis.no.cluster") 函数
        * 首先会去app.properties这个文件中读取出信息，之后将在app.properties拿到的信息再去pom.xml文件当中取查询
        */
        String noCluster = ConfigUtils.getProperty("client.redis.no.cluster"); //gcs:这个noCluster =true
        if (noCluster != null && noCluster.equals("true")) {
            return getInstancesFromProperties(null);
        } else {
            return getClusterInstancesFromProperties(null);
        }
    }

    /**<br>gcs:<br>
     * 创建一个redis的对象
     * @param prefix 前缀
     * @return  获得一个RedisCache对象
     * */
    public static RedisCache getInstances(String prefix) {
        if ("default".equals(prefix)) { //gcs:确定prefix是否等价与default
            return getInstances();  //gcs:如果这个prefix为默认的名字，此时就会默认创建一个Instances对象
        }

        //gcs:从pom文件中将这个名字为 client.redis.no.cluster 的配置信息提取出来。为什么我什么也没有提取到呢
        String noCluster = ConfigUtils.getProperty("client.redis.no.cluster");
        if (noCluster != null && noCluster.equals("true")) { //gcs:如果获得的noCluster等于true，此时就会返回一个RedisCache的对象
            return getInstancesFromProperties(prefix);
        } else {
            return getClusterInstancesFromProperties(prefix);
        }
    }

    /**
    * <b>author:</b> gcs <br>
    * <b>data:</b> 18-7-10 <br>
    * <b>description:</b><br>
     *     这个函数是什么作用啊???
     *     这个prefix（前缀）是什么意思啊？？？
     *     这个prefix是为了创建多个Redis的实例的。加了一个prefix之后就可以启动多个Redis了。
     *     比方说我们一个Redis是100G，但是因为Redis是单线程的，所以Redis就必须排着队取使用。但是如果我们起多个名字
     *     将100G的Redis分成10个的10G的Redis，这样就会好很多了。
    * <b>param:</b><br>
    * <b>return:</b><br>
    */
    public synchronized static RedisCache getInstancesFromProperties(String perfix) {

        //==========================================================1
        /*
        *gcs:
        *反正这里进行一系列的Redis的操作
        */
        //gcs:这是什么啊???
        String writeMaxIdle = "client.redis.write.pool.maxIdle";
        if (!StringUtils.isEmpty(perfix)) writeMaxIdle = perfix + "." + writeMaxIdle; //gcs:如果prefix不是空的话，此时会进行什么操作？？
        String writeTestOnBorrow = "client.redis.pool.testOnBorrow";
        if (!StringUtils.isEmpty(perfix)) writeTestOnBorrow = perfix + "." + writeTestOnBorrow;
        String writePoolHost = "client.redis.write.pool";  //gcs:获得redis的线程池的Host的地址
        if (!StringUtils.isEmpty(perfix)) writePoolHost = perfix + "." + writePoolHost;
        String writePoolHostPassword = "client.redis.write.pool.password";
        if (!StringUtils.isEmpty(perfix)) writePoolHostPassword = perfix + "." + writePoolHostPassword;

        String readMaxIdle = "client.redis.write.pool.maxIdle";
        if (!StringUtils.isEmpty(perfix)) readMaxIdle = perfix + "." + readMaxIdle;
//        String readTestOnBorrow = "client.redis.pool.testOnBorrow";
//        if (!StringUtils.isEmpty(perfix)) writeMaxIdle=perfix+"."+readTestOnBorrow;
        String readPoolHost = "client.redis.write.pool";
        if (!StringUtils.isEmpty(perfix)) readPoolHost = perfix + "." + readPoolHost;
        String readPoolHostPassword = "client.redis.write.pool.password";
        if (!StringUtils.isEmpty(perfix)) readPoolHostPassword = perfix + "." + readPoolHostPassword;


            try {
                RedisCacheImpl redisCache = new ClientRedisCache();
                GenericObjectPoolConfig readPoolConfig = new GenericObjectPoolConfig();
                JedisClientPool readPool = null;

                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();

                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty(writeMaxIdle)));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty(writeTestOnBorrow)));
                writePoolConfig.setMaxWaitMillis(480000);
                JedisClientPool writePool = new JedisClientPool(readPoolConfig, ConfigUtils.getProperty(writePoolHost), ConfigUtils.getProperty(writePoolHostPassword), 480000);
                if (ConfigUtils.getProperty(readPoolHost) != null) {
                    readPoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty(readMaxIdle)));
                    readPoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty(writeTestOnBorrow)));
                    readPoolConfig.setMaxWaitMillis(480000);
                    readPool = new JedisClientPool(readPoolConfig, ConfigUtils.getProperty(readPoolHost), ConfigUtils.getProperty(readPoolHostPassword), 480000);
                } else {
                    readPool = writePool;
                }
                redisCache.setPool(writePool);
                redisCache.setReadPool(readPool);
                CacheClientFactory.redisCache = redisCache;
                return redisCache;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

    }

    public synchronized static RedisCache getClusterInstancesFromProperties(String perfix) {
        String writeMaxIdle = "client.redis.write.pool.maxIdle";
        if (!StringUtils.isEmpty(perfix)) writeMaxIdle = perfix + "." + writeMaxIdle;
        String writeTestOnBorrow = "client.redis.pool.testOnBorrow";
        if (!StringUtils.isEmpty(perfix)) writeTestOnBorrow = perfix + "." + writeTestOnBorrow;
        String writePoolHost = "client.redis.write.pool";
        if (!StringUtils.isEmpty(perfix)) writePoolHost = perfix + "." + writePoolHost;
        String writePoolHostPassword = "client.redis.write.pool.password";
        if (!StringUtils.isEmpty(perfix)) writePoolHostPassword = perfix + "." + writePoolHostPassword;


            try {
                Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
                String nodesstr = ConfigUtils.getProperty(writePoolHost);
                String[] nodes = nodesstr.split("[,]");
                for (String nodestr : nodes) {
                    String node[] = nodestr.split(":");
                    jedisClusterNodes.add(new HostAndPort(node[0], Integer.parseInt(node[1])));
                }
                RedisClusterImpl redisCache = new RedisClusterImpl();
                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();
                writePoolConfig.setMaxWaitMillis(360000);
                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty(writeMaxIdle)));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty(writeTestOnBorrow)));
                JedisCluster jc = new JedisCluster(jedisClusterNodes, 360000, 10000, writePoolConfig);
                redisCache.setCluster(jc);
                CacheClientFactory.redisCache = redisCache;
                return redisCache;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

    }

    public static void main(String[] args) {
        CacheClientFactory.getInstancesFromProperties("").add("mikkkkk", "ffff", 10000);
    }
}
