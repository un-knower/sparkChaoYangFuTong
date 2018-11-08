package com.ald.cache.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ald.cache.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by luanma on 2017/5/14.
 */
public class RedisCacheImpl implements RedisCache {
    private static final Logger logger = LoggerFactory.getLogger(RedisCacheImpl.class);

    private static final String SUCCESS = "OK";
    private static final int lockTimeout = 30000;
    private static final String LOCK_PREFIX = "lock.";
    private static final String KEY_PREFIX_LOCK = "lock:";
    private static final int LOCK_EXPIRE_TIME = 45;
    private static final long WAIT_TIME = 1000L;
    private final Map<String, AtomicInteger> keyLocks = new HashMap<String, AtomicInteger>();
    private ShardedJedisPool pool;
    private ShardedJedisPool readPool;

    /**
     * the pool to set
     *
     * @param pool
     */
    public void setPool(ShardedJedisPool pool) {
        this.pool = pool;
    }

    public void setReadPool(ShardedJedisPool pool) {
        this.readPool = pool;
    }

    public void addString(String key, String value, int expiration) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                if (expiration <= 0) {
                    resource.set(key, value);
                } else {
                    resource.setex(key, expiration, value);
                }

            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public String getString(String key) {
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                return resource.get(key);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public void add(String key, Object value, int expiration) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                resource.del(key);
                String str = JSON.toJSONString(value, SerializerFeature.WriteClassName);
                if (expiration <= 0) {
                    resource.set(key, str);
                } else {
                    resource.setex(key, expiration, str);
                }
            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public Object get(String key) {
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                String str = resource.get(key);
                if (null != str) {
                    return JSON.parse(str);
                }
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public void delete(String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                resource.del(key);
            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public Map<String, Object> get(String... keys) {
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            Map<String, Object> map = new HashMap<String, Object>(keys.length);
            try {
                for (String key : keys) {
                    String str = resource.get(key);
                    if (null != str) {
                        Object obj = JSON.parse(str);
                        map.put(key, obj);
                    }
                }
                return map;
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public <T> T get(String key, Class<T> value) {
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                String str = resource.get(key);
                if (null != str) {
                    return JSON.parseObject(str, value);
                }
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public long decr(String key, int by, long def) {
        ShardedJedis resource = pool.getResource();
        long result = def;

        if (resource != null) {
            try {
                result = resource.decrBy(key, by);
            } finally {
                pool.returnResource(resource);
            }
        }
        return result;
    }

    @Override
    public synchronized long incr(String key, int by, long def) {
        ShardedJedis resource = pool.getResource();
        long result = def;

        if (resource != null) {
            try {
                String val = resource.get(key);
                if (val != null) {
                    result = resource.incrBy(key, by);
                } else {
                    resource.set(key, def + "");
                    result = resource.incrBy(key, by);

                }
            } finally {
                pool.returnResource(resource);
            }
        }
        return result;
    }

    @Override
    public boolean safeAdd(String key, Object value, int expiration) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                if (value instanceof Serializable) {
                    String str = JSON.toJSONString(value, SerializerFeature.WriteClassName);
                    String ret = null;
                    if (expiration <= 0) {
                        ret = resource.set(key, str);
                    } else {
                        ret = resource.setex(key, expiration, str);
                    }
                    return SUCCESS.equals(ret);
                }
            } finally {
                pool.returnResource(resource);
            }
        }
        return false;
    }

    @Override
    public boolean safeDelete(String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.del(key) > 0;
            } finally {
                pool.returnResource(resource);
            }
        }
        return false;
    }

    @Override
    public boolean safeSet(String key, Object value, int arg2) {
        return safeAdd(key, value, arg2);
    }

    @Override
    public void set(String key, Object value, int arg2) {
        safeSet(key, value, arg2);
    }

    @Override
    public void clear() {
        Jedis jedis = getShard();
        jedis.flushDB();
    }

    @Override
    public long llen(String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.llen(key);
            } finally {
                pool.returnResource(resource);
            }
        }
        return 0;
    }

    @Override
    public List<String> getList(String key, int end) {
        ShardedJedis resource = readPool.getResource();
        List<String> list = null;
        if (resource != null) {
            try {
                if (end <= 0) {
                    list = resource.lrange(key, 0, -1);
                } else {
                    list = resource.lrange(key, 0, end);
                }

            } finally {
                readPool.returnResource(resource);
            }
        }
        return list;
    }

    @Override
    public void setList(String key, List<String> list) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                for (String l : list) {
                    resource.lpush(key, l);
                }

            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public void lpush(String key, String data) {
        try {
            ShardedJedis resource = pool.getResource();
            if (resource != null) {
                try {
                    resource.lpush(key, data);
                } finally {
                    pool.returnResource(resource);
                }
            }
        } catch (Exception e) {
            logger.error("list cache push error!!!");
        }
    }

    @Override
    public void lpush(List<String> list, String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                for (String l : list) {
                    resource.lpush(key, l);
                }

            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public String rpop(String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                while (true) {
                    if (!resource.exists(key)) {
                        synchronized (this) {
                            wait(15);
                            continue;
                        }
                    }
                    return resource.rpop(key);
                }

            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public <T> void lpush(String key, List<T> list) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                for (T l : list) {
                    resource.lpush("OBJ:" + key, JSON.toJSONString(l, SerializerFeature.WriteClassName));
                }
            } finally {
                pool.returnResource(resource);
            }
        }

    }

    @Override
    public <T> T rpop(String key, Class<T> clazz) {
        ShardedJedis resource = pool.getResource();
        String k = "OBJ:" + key;
        if (resource != null) {
            try {
                while (true) {
                    if (!resource.exists(k) || null == resource.rpop(k)) {
                        synchronized (this) {
                            wait(200);
                            continue;
                        }
                    }
                    return JSON.parseObject(resource.rpop(k), clazz);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public void addSet(String key, Set<String> set) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                for (String l : set) {
                    resource.sadd(key, l);
                }

            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public Set<String> getSet(String key) {
        ShardedJedis resource = readPool.getResource();
        Set<String> st = null;
        if (resource != null) {
            try {
                st = resource.smembers(key);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return st;
    }

    @Override
    public void addHset(String key, String fieldKey, String fieldValue) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                resource.hset(key, fieldKey, fieldValue);
            } finally {
                pool.returnResource(resource);
            }
        }

    }

    @Override
    public String getHget(String key, String fieldKey) {
        ShardedJedis resource = readPool.getResource();
        String rt = null;
        if (resource != null) {
            try {
                rt = resource.hget(key, fieldKey);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return rt;
    }

    @Override
    public void addHmset(String key, Map<String, String> field) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                resource.hmset(key, field);
            } finally {
                pool.returnResource(resource);
            }
        }

    }

    @Override
    public List<String> getHmget(String key, String[] fieldKey) {

        ShardedJedis resource = readPool.getResource();
        List<String> rt = null;
        if (resource != null) {
            try {
                rt = resource.hmget(key, fieldKey);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return rt;
    }

    @Override
    public long hlen(String key) {
        long d = 0;
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                d = resource.hlen(key);
            } finally {
                pool.returnResource(resource);
            }
        }
        return d;
    }

    @Override
    public void hset2Object(String key, String fieldKey, Object obj) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                if (obj instanceof Serializable) {
                    String str = JSON.toJSONString(obj, SerializerFeature.WriteClassName);
                    resource.hset(key, fieldKey, str);
                }
            } finally {
                pool.returnResource(resource);
            }
        }

    }

    @Override
    public <T> T hget2Object(String key, String fieldKey, Class<T> clazz) {
//        logger.debug("readPool:" + readPool);
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                String string = resource.hget(key, fieldKey);
//                logger.debug("cached: " + string);
                if (null != string) {
                    return JSON.parseObject(string, clazz);
                }
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public <T> void addHmset2Object(String key, Map<String, T> field) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                Iterator<Map.Entry<String, T>> iter = field.entrySet().iterator();
                Map<byte[], byte[]> hash = new HashMap<byte[], byte[]>();
                while (iter.hasNext()) {
                    Map.Entry<String, T> entry = iter.next();
                    String k = entry.getKey();
                    Object obj = entry.getValue();
                    String str = JSON.toJSONString(obj, SerializerFeature.WriteClassName);
                    hash.put(k.getBytes(), str.getBytes());
                }
                resource.hmset(key.getBytes(), hash);

            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public <T> List<T> getHmget2Object(String key, String[] fieldKey, Class<T> clazz) {
        System.out.println("readPool:" + readPool);
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {

                List<String> strList = resource.hmget(key, fieldKey);

                return JSONArray.parseArray(strList.toString(), clazz);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return new ArrayList<T>();
    }

    @Override
    public Map<String, String> getHmall(String key) {
        ShardedJedis resource = readPool.getResource();
        Map<String, String> map = null;
        if (resource != null) {
            try {
                map = resource.hgetAll(key);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return map;
    }

    @Override
    public void hdel(String key, String[] fieldKeys) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                for (String fkey : fieldKeys) {
                    resource.hdel(key, fkey);
                }
            } finally {
                pool.returnResource(resource);
            }
        }


    }

    @Override
    public Set<String> hkeys(String key) {
        ShardedJedis resource = readPool.getResource();
        Set<String> rt = null;
        if (resource != null) {
            try {
                rt = resource.hkeys(key);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return rt;
    }

    @Override
    public List<String> hvals(String key) {
        ShardedJedis resource = readPool.getResource();
        List<String> rt = null;
        if (resource != null) {
            try {
                rt = resource.hvals(key);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return rt;
    }

    @Override
    public void add2Hsetnx(String value, String... key) throws IllegalArgumentException {
        String[] ks = findKey(key);
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                resource.hset(ks[0], ks[1], value);
            } finally {
                pool.returnResource(resource);
            }
        }
    }

    private String[] findKey(String... key) {
        if (key.length == 2) {
            return key;
        } else if (key.length > 2) {
            String[] k = new String[key.length - 1];
            for (int i = 0; i < key.length - 1; i++) {
                k[i] = key[i];
            }
            return new String[]{findIndex(k), key[key.length - 1]};
        }

        return null;
    }

    private String findIndex(String[] k) {
        if (k.length == 1)
            return k[0];
        else {
            ShardedJedis resource = readPool.getResource();
            if (resource != null) {
                try {
                    String index = resource.hget(k[0], k[1]);
                    if (null == index)
                        throw new IllegalArgumentException("key is illegal argument");
                    String[] tmp = new String[k.length - 1];
                    tmp[0] = index;
                    for (int i = 1; i < k.length - 1; i++) {
                        tmp[i] = k[i + 1];
                    }
                    return findIndex(tmp);
                } finally {
                    readPool.returnResource(resource);
                }
            }
        }

        return null;
    }

    @Override
    public String get2Hget(String... key) throws IllegalArgumentException {
        String[] ks = findKey(key);
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                return resource.hget(ks[0], ks[1]);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public void hincrby(String key, String field, long increment) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                resource.hincrBy(key, field, increment);
            } finally {
                pool.returnResource(resource);
            }
        }
    }

    @Override
    public Long setnx(String key, String value) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.setnx(key, value);
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.setnx(key, value);
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Long del(String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.del(key);
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public byte[] getset(byte[] key, byte[] value) {
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                return resource.getSet(key, value);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public String getset(String key, String value) {
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                return resource.getSet(key, value);
            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Long expire(String key, int seconds) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.expire(key, seconds);
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.expire(key, seconds);
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Jedis getShard(String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.getShard(key);
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Jedis getShard(byte[] key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.getShard(key);
            } finally {
                pool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Jedis getShard() {
        ShardedJedis resource = pool.getResource();
        Jedis jedis = null;
        if (resource != null) {
            try {
                for (Jedis j : resource.getAllShards()) {
                    if (null != j) {
                        jedis = j;
                        break;
                    }
                }
            } finally {
                pool.returnResource(resource);
            }
        }
        return jedis;
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        //ScanParams.SCAN_POINTER_START
        Set<String> set = new HashSet<String>();
        ShardedJedis resource = readPool.getResource();

        if (resource != null) {
            try {
                ScanResult<Map.Entry<String, String>> scanResult = resource.hscan(key, cursor);
                return scanResult;
//                for (Jedis jedis : resource.getAllShards()) {
//                    set.addAll(jedis.keys(key));
//                }

            } finally {
                readPool.returnResource(resource);
            }
        }
        return null;
    }

    @Override
    public Set<String> keys(String key) {
        Set<String> set = new HashSet<String>();
        ShardedJedis resource = readPool.getResource();
        if (resource != null) {
            try {
                for (Jedis jedis : resource.getAllShards()) {
                    set.addAll(jedis.keys(key));
                }

            } finally {
                readPool.returnResource(resource);
            }
        }
        return set;
    }

    @Override
    public boolean hExists(String key, String field) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.hexists(key, field);
            } finally {
                pool.returnResource(resource);
            }
        }
        return false;
    }

    @Override
    public boolean exists(String key) {
        ShardedJedis resource = pool.getResource();
        if (resource != null) {
            try {
                return resource.exists(key);
            } finally {
                pool.returnResource(resource);
            }
        }
        return false;
    }

    /**
     * random_key = getrandomkey()
     * value = redis.setnx(key, random_key)
     * if value == 1:
     * redis.expire(key, timeout)
     * do_job()
     * redis.watch(mykey)
     * value = redis.get(mykey)
     * if random_key == value:
     * redis.multi()
     * redis.del(mykey)
     * redis.exec
     * else:
     * # not the lock itself, so can not be removed
     * else:
     * # don't get lock
     *
     * @param key
     * @param ttl
     * @param ilock
     * @throws Throwable
     */

    public void lock(String key, int ttl, InterLockWorker ilock) throws Throwable {
        Jedis jedis = getShard();
        String randomValue = UUID.randomUUID().toString();
        long lock = 0;
        while (lock != 1) {
            lock = jedis.setnx(LOCK_PREFIX + key, randomValue);
            jedis.expire(LOCK_PREFIX + key, ttl);
            if (lock == 1) {
                ilock.call();
                jedis.watch(LOCK_PREFIX + key);
                String rValue = jedis.get(LOCK_PREFIX + key);
                if (randomValue.equals(rValue)) {
                    Transaction transaction = jedis.multi();
                    jedis.del(LOCK_PREFIX + key);
                    if (null != transaction.exec())
                        break;
                } else {
                    logger.warn("lock value is expire or call method time-consuming");
                }
            } else {
                Thread.sleep(10);
                continue;
            }
        }
    }

    private Object getKeyLock(String key) {
        AtomicInteger result;
        synchronized (keyLocks) {
            result = keyLocks.get(key);
            if (result == null) {
                keyLocks.put(key, result = new AtomicInteger());
            }
            result.incrementAndGet();
        }
        return result;
    }

    private Object getAndReleaseKeyLock(String key) {
        AtomicInteger result;
        synchronized (keyLocks) {
            result = keyLocks.get(key);
            if (result.decrementAndGet() == 0) {
                keyLocks.remove(key);
            }
        }
        return result;
    }

    public void lock(String key) {
        long lockMark = 0L;
        String lockKey = KEY_PREFIX_LOCK + key;
        Object lock = getKeyLock(lockKey);
        synchronized (lock) {
            while (lockMark != 1L) {
                ShardedJedis resource = pool.getResource();
                try {
                    lockMark = resource.setnx(lockKey, "A");
                    if (lockMark == 1L) {
                        resource.expire(lockKey, LOCK_EXPIRE_TIME);
                        lock.notify();
                        break;
                    }
                } finally {
                    pool.returnResource(resource);
                }

                try {
                    lock.wait(WAIT_TIME);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    public void unlock(String key) {
        String lockKey = KEY_PREFIX_LOCK + key;
        Object lock = getAndReleaseKeyLock(lockKey);
        synchronized (lock) {
            ShardedJedis resource = pool.getResource();
            try {
                resource.del(lockKey);
                lock.notifyAll();
            } finally {
                pool.returnResource(resource);
            }
        }
    }

}
