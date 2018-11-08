package com.ald.stat.cache;

import com.ald.stat.cache.impl.RedisCacheImpl;
import redis.clients.jedis.ShardedJedis;

import java.io.Closeable;
import java.io.IOException;

public class ClientRedisCache extends RedisCacheImpl  {
    public ShardedJedis getResource() {
        return pool.getResource();
    }
    @Override
    public void close() throws IOException {
//        try {
//            if (pool != null)
//                pool.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
