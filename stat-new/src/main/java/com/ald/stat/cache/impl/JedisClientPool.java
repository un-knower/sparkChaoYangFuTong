package com.ald.stat.cache.impl;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.net.URI;
import java.util.List;
import java.util.Vector;

public class JedisClientPool extends ShardedJedisPool {

    public JedisClientPool(GenericObjectPoolConfig poolConfig, List<JedisShardInfo> shards) {
        super(poolConfig, shards);
    }

    public JedisClientPool(GenericObjectPoolConfig poolConfig, String serverlist) {
        super(poolConfig, toList(serverlist));
    }

    public JedisClientPool(GenericObjectPoolConfig poolConfig, String serverlist, int timeout) {
        super(poolConfig, toList(serverlist, null, timeout));
    }

    public JedisClientPool(GenericObjectPoolConfig poolConfig, String serverlist, String password) {
        super(poolConfig, toList(serverlist, password, 180000));
    }

    public JedisClientPool(GenericObjectPoolConfig poolConfig, String serverlist, String password, int timeout) {
        super(poolConfig, toList(serverlist, password, timeout));
    }

    private static List<JedisShardInfo> toList(String serverlist) {
        return toList(serverlist, null, 180000);
    }

    private static List<JedisShardInfo> toList(String serverlist, String password, int timeout) {
        String[] servers = serverlist.split("[,]");

        List<JedisShardInfo> list = new Vector<JedisShardInfo>(servers.length);
        for (String hostandport : servers) {
            if (hostandport.startsWith("redis://")) {
                try {
                    list.add(new JedisShardInfo(new URI(hostandport)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                String[] strs = hostandport.split(":");
                String host = strs[0];
                int port = 6379;
                if (strs.length > 1) {
                    port = Integer.parseInt(strs[1]);
                }
                JedisShardInfo jsi = new JedisShardInfo(host, port, timeout);
                jsi.setSoTimeout(timeout);
                if (password != null) {
                    jsi.setPassword(password);
                }
                list.add(jsi);
            }
        }
        return list;
    }
}
