package com.ald.stat.cache;

import com.ald.stat.cache.impl.RedisCacheImpl;
import redis.clients.jedis.ShardedJedis;

public class ClientRedisCache extends RedisCacheImpl {
    public ShardedJedis getResource(){
       return pool.getResource();
    }
}
