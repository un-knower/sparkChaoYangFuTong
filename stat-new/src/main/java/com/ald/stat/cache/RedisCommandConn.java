package com.ald.stat.cache;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Protocol;

/**
 * Created by zhaofw on 2018/7/30.
 */
public class RedisCommandConn extends Connection {
    public RedisCommandConn(){
        super("10.0.220.11",6379);
    }

    public RedisCommandConn(String host, int port){
        super(host,port);
    }

    @Override
    public Connection sendCommand(Protocol.Command cmd, String... args) {
        return super.sendCommand(cmd, args);
    }
}
