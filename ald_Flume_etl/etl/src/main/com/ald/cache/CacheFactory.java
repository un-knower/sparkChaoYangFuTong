package com.ald.cache;

import com.ald.cache.impl.JedisClientPool;
import com.ald.cache.impl.RedisCacheImpl;
import com.ald.cache.impl.RedisClusterImpl;
import com.ald.util.ConfigUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by luanma on 2017/5/14.
 */
public class CacheFactory {
    private static final Logger logger = LoggerFactory.getLogger(CacheFactory.class);
    private static RedisCache redisCache;

    public static RedisCache getInstances() {
        String cluster = ConfigUtils.get("redis.cluster");
        if ("true".equalsIgnoreCase(cluster) || "yes".equalsIgnoreCase(cluster)) {
            return getClusterInstancesFromProperties();
        } else {
            return getInstancesFromProperties();
        }
    }

    public synchronized static RedisCache getInstancesFromProperties() {
        if (CacheFactory.redisCache == null) {
            try {
                RedisCacheImpl redisCache = new RedisCacheImpl();
                GenericObjectPoolConfig readPoolConfig = new GenericObjectPoolConfig();
                JedisClientPool readPool = null;

                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();
                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.get("redis.write.pool.maxIdle")));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.get("redis.pool.testOnBorrow")));
                JedisClientPool writePool = new JedisClientPool(readPoolConfig, ConfigUtils.get("redis.write.pool"));

                if (ConfigUtils.get("redis.read.pool") != null) {
                    readPoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.get("redis.read.pool.maxIdle")));
                    readPoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.get("redis.pool.testOnBorrow")));
                    readPool = new JedisClientPool(readPoolConfig, ConfigUtils.get("redis.read.pool"));
                } else {
                    readPool = writePool;
                }

                System.out.println(ConfigUtils.get("redis.read.pool.maxIdle") + "-----");
                redisCache.setPool(writePool);
                redisCache.setReadPool(readPool);
                CacheFactory.redisCache = redisCache;
                return redisCache;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        } else {
            return redisCache;
        }
    }

    public synchronized static RedisCache getClusterInstancesFromProperties() {
        if (CacheFactory.redisCache == null) {
            try {
                Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
                String nodesstr = ConfigUtils.get("redis.write.pool");
                String[] nodes = nodesstr.split("[,]");
                for (String nodestr : nodes) {
                    String node[] = nodestr.split(":");
                    jedisClusterNodes.add(new HostAndPort(node[0], Integer.parseInt(node[1])));
                }
                RedisClusterImpl redisCache = new RedisClusterImpl();
                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();
                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.get("redis.write.pool.maxIdle")));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.get("redis.pool.testOnBorrow")));
                JedisCluster jc = new JedisCluster(jedisClusterNodes, 5000, 1000, writePoolConfig);
                redisCache.setCluster(jc);
                System.out.println(ConfigUtils.get("redis.write.pool.maxIdle") + "-----");
                CacheFactory.redisCache = redisCache;
                return redisCache;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        } else {
            return redisCache;
        }
    }

    public static void main(String[] args) {
        CacheFactory.getInstancesFromProperties();
    }
}
