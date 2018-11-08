package com.ald.stat.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;

public class TestJedis {
    public static void main(String[] args) {
//        try (JedisPool jedisPool = new JedisPool("localhost", 6379)) {
//            Jedis jedis = null;
//            try {
//                jedis = jedisPool.getResource();
//                jedis.set("rediskey1", "redisvalue1");
//                jedis.set("rediskey2", "redisvalue2");
//                System.out.println(jedis.get("rediskey1"));
//                System.out.println(jedis.get("rediskey2"));
//            } catch (Exception e) {
//                e.printStackTrace();
//            } finally {
//                if (jedis != null) {
//                    jedis.close();
//                }
//            }
//            jedisPool.destroy();
//        }

        /*TestRedisConn conn = new TestRedisConn();
        try {
            conn.sendCommand(Protocol.Command.AUTH,"aldwxredis123");
            conn.sendCommand(Protocol.Command.KEYS,"*","faa811295d66a4bb53f941fa6246bb00a3e13cb3");
            List<String> key_list = conn.getMultiBulkReply();
            for (String s : key_list) {
                System.out.println(s);
            }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }*/

        RedisCommandConn conn = RedisCommandFactory.getInstances("jobLink");
        try {
            conn.sendCommand(Protocol.Command.AUTH, "aldwxredis123");
            String strStatus = conn.getStatusCodeReply();
            System.out.println("auth result:" + strStatus);

            conn.sendCommand(Protocol.Command.CLUSTER, "nodes");
            String strNodeList = conn.getBulkReply();
            System.out.println("node:" + strNodeList);
            String[] nodeList = strNodeList.split("\n");
            for(int i = 0; i < nodeList.length; i++)
            {
                String[] nodeInfo_arr = nodeList[i].split(" ");
                if (nodeInfo_arr.length>3){
                    String nodeId = nodeInfo_arr[0];
                    System.out.println("nodeId："+nodeId);
                    String nodeType = nodeInfo_arr[2];//节点类型 master | salve
                    System.out.println("nodeType："+nodeType);
                    if (nodeType.equals("master")){
                        conn.sendCommand(Protocol.Command.KEYS, "*", nodeId);
                        List<String> keyList = conn.getMultiBulkReply();
                        for (String key:keyList) {
                            //System.out.println(key);
                            conn.sendCommand(Protocol.Command.DEL,key);
                        }
                        System.out.println("size"+keyList.size());
                    }
                }

//                int iIndex = nodeList[i].indexOf(" ");
//                String strNodeID = nodeList[i].substring(0, iIndex);
//                System.out.println(strNodeID);
//
//                conn.sendCommand(Protocol.Command.KEYS, "*", strNodeID);
//                List<String> keyList = conn.getMultiBulkReply();
//                System.out.println("size:" + keyList.size() );
            }

        } catch (JedisConnectionException jce) {
            System.out.println(jce);
        }

    }
}
