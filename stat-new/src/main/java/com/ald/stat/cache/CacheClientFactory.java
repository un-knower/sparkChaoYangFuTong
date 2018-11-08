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
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: liyj12
 * Date: 2015/2/6
 * Time: 16:00
 */
public class CacheClientFactory {

    static ConcurrentHashMap<String, RedisCache> redisCaches = new ConcurrentHashMap();
    //static RedisCache redisCache;

    public static RedisCache getInstances() {
        String noCluster = ConfigUtils.getProperty("client.redis.no.cluster");
        if (noCluster != null && noCluster.equals("true")) {
            return getInstancesFromProperties(null);
        } else {
            return getClusterInstancesFromProperties(null);
        }
    }

    public static RedisCache getInstances(String prefix) {

        if ("default".equals(prefix)) {
            return getInstances();
        }
        String noCluster = ConfigUtils.getProperty("client.redis.no.cluster");
        if (noCluster != null && noCluster.equals("true")) {
            return getInstancesFromProperties(prefix);
        } else {
            return getClusterInstancesFromProperties(prefix);
        }
    }

    public static RedisCache getInstances(String prefix, String isCluster) {

        if ("default".equals(prefix)) {
            return getInstances();
        }
        String noCluster = ConfigUtils.getProperty(prefix + "." + "client.redis.no.cluster");
        if (noCluster != null && noCluster.equals("true")) {
            return getInstancesFromProperties(prefix);
        } else {
            return getClusterInstancesFromProperties(prefix);
        }
    }

    public synchronized static RedisCache getInstancesFromProperties(String perfix) {
        String writeMaxIdle = "client.redis.write.pool.maxIdle";
        if (!StringUtils.isEmpty(perfix)) writeMaxIdle = perfix + "." + writeMaxIdle;
        String writeTestOnBorrow = "client.redis.pool.testOnBorrow";
        if (!StringUtils.isEmpty(perfix)) writeTestOnBorrow = perfix + "." + writeTestOnBorrow;
        String writePoolHost = "client.redis.write.pool";
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

        String poolId = "default";
        if (StringUtils.isNotEmpty(perfix)) {
            poolId = perfix;
        }

        if (redisCaches.get(poolId) == null) {
            try {
                RedisCacheImpl redisCache = new ClientRedisCache();
                GenericObjectPoolConfig readPoolConfig = new GenericObjectPoolConfig();
                JedisClientPool readPool = null;

                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();

                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty(writeMaxIdle)));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty(writeTestOnBorrow)));
                writePoolConfig.setMaxWaitMillis(480000);
                String host = ConfigUtils.getProperty(writePoolHost);
                //System.out.println("host:" + host + " password:" + ConfigUtils.getProperty(writePoolHostPassword));
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
                redisCaches.put(poolId, redisCache);
                //CacheClientFactory.redisCache = redisCache;
                return redisCaches.get(poolId);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            return redisCaches.get(poolId);
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
        String poolId = "default";
        if (StringUtils.isNotEmpty(perfix)) {
            poolId = perfix;
        }
        if (redisCaches.get(poolId) == null) {
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
                //Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout,int maxAttempts, String password, final GenericObjectPoolConfig poolConfig
                JedisCluster jc = new JedisCluster(jedisClusterNodes, 360000, 10000, 3, ConfigUtils.getProperty(writePoolHostPassword), writePoolConfig);
                redisCache.setCluster(jc);
                redisCaches.put(poolId, redisCache);
                //CacheClientFactory.redisCache = redisCache;
                return redisCaches.get(poolId);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            return redisCaches.get(poolId);
        }
    }

    public void release() {
        redisCaches.values().forEach(redisCache -> {
            try {
                if (redisCache != null)
                    redisCache.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args) {
        CacheClientFactory.getInstancesFromProperties("").add("mikkkkk", "ffff", 10000);
    }
}
