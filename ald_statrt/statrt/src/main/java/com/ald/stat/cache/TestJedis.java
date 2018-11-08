package com.ald.stat.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TestJedis {
    public static void main(String[] args) {
        try (JedisPool jedisPool = new JedisPool("localhost", 6379)) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                jedis.set("rediskey1", "redisvalue1");
                jedis.set("rediskey2", "redisvalue2");
                System.out.println(jedis.get("rediskey1"));
                System.out.println(jedis.get("rediskey2"));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
            jedisPool.destroy();
        }
    }
}
