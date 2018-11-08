package com.ald.stat.cache;


import com.ald.stat.cache.impl.JedisClientPool;
import com.ald.stat.cache.impl.RedisCacheImpl;
import com.ald.stat.cache.impl.RedisClusterImpl;
import com.ald.stat.utils.ConfigUtils;
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
public class CacheFactory {
    static RedisCache redisCache;

    public static RedisCache getInstances() {
        String noCluster = ConfigUtils.getProperty("redis.no.cluster");
        if (noCluster != null && noCluster.equals("true")) {
            return getInstancesFromProperties();
        } else {
            return getClusterInstancesFromProperties();
        }
    }

    public synchronized static RedisCache getInstancesFromProperties() {
        if (CacheFactory.redisCache == null) try {
            RedisCacheImpl redisCache = new RedisCacheImpl();
            GenericObjectPoolConfig readPoolConfig = new GenericObjectPoolConfig();
            JedisClientPool readPool = null;

            GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();

            writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty("redis.write.pool.maxIdle")));
            writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty("redis.pool.testOnBorrow")));
            JedisClientPool writePool = new JedisClientPool(readPoolConfig, ConfigUtils.getProperty("redis.write.pool"), ConfigUtils.getProperty("redis.write.pool.password"));

            if (ConfigUtils.getProperty("redis.read.pool") != null) {
                readPoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty("redis.read.pool.maxIdle")));
                readPoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty("redis.pool.testOnBorrow")));
                readPool = new JedisClientPool(readPoolConfig, ConfigUtils.getProperty("redis.read.pool"), ConfigUtils.getProperty("redis.write.pool.password"));
            } else {
                readPool = writePool;
            }
            redisCache.setPool(writePool);
            redisCache.setReadPool(readPool);
            CacheFactory.redisCache = redisCache;
            return redisCache;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        else {
            return redisCache;
        }
    }

    public synchronized static RedisCache getClusterInstancesFromProperties() {
        if (CacheFactory.redisCache == null) {
            try {
                Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
                String nodesstr = ConfigUtils.getProperty("redis.write.pool");
                String[] nodes = nodesstr.split("[,]");
                for (String nodestr : nodes) {
                    String node[] = nodestr.split(":");
                    jedisClusterNodes.add(new HostAndPort(node[0], Integer.parseInt(node[1])));
                }
                RedisClusterImpl redisCache = new RedisClusterImpl();
                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();
                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty("redis.write.pool.maxIdle")));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty("redis.pool.testOnBorrow")));
                JedisCluster jc = new JedisCluster(jedisClusterNodes, 5000, 1000, writePoolConfig);
                redisCache.setCluster(jc);
//                System.out.println(ConfigUtils.getProperty("redis.write.pool.maxIdle") + "-----");
                CacheFactory.redisCache = redisCache;
                return redisCache;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            return redisCache;
        }
    }

    public static void main(String[] args) {
        RedisCache instancesFromProperties = CacheFactory.getInstancesFromProperties();
        instancesFromProperties.set("name1100", "mike1", 1000);
    }
}
