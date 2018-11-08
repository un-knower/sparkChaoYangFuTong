package com.ald.finder.baidu;

import com.ald.GeoFinder;

/**
 * Created by luanma on 2017/5/14.
 */
public interface GeoFromWeb extends GeoFinder {
    /**
     * 连接池里的最大连接数
     */
    int MAX_TOTAL_CONNECTIONS = 200;

    /**
     * 每个路由的默认最大连接数
     */
    int MAX_ROUTE_CONNECTIONS = 200;

    /**
     * 连接超时时间
     */
    int CONNECT_TIMEOUT = 5000;

    /**
     * 套接字超时时间
     */
    int SOCKET_TIMEOUT = 10000;

    /**
     * 连接池中 连接请求执行被阻塞的超时时间
     */
    long CONN_MANAGER_TIMEOUT = 5000;
}
