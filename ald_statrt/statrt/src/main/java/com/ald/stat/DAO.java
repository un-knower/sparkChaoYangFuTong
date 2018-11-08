/**
 * @version 1.01
 * 在needLog方法中加入了一些固定判断。
 * 今后将升级为从配置文件里读取。
 */
package com.ald.stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


//import weblogic.jndi.*;
//import weblogic.common.*;

public class DAO {
    private final static Logger logger = LoggerFactory.getLogger(DAO.class);
    private static String dbDriver = null;
    private static String dbUrl = null;
    private static String dbUser = null;
    private static String dbPassword = null;
    public static boolean usePool = false;
    private static Map dataSources = Collections.synchronizedMap(new HashMap());

    public static Context getInitialContext() throws NamingException {
        Context ctx = new InitialContext();
        return ctx;
    }

    /**
     * 描述： 取缺省的数据库属性，根据设置文件
     */
    private static void getDBProp() throws SQLException {
        try {
            dbUrl = "";
            dbDriver = "";
            dbUser = "user";
            dbPassword = "user";
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 数据库连接使用Pool
     */
    public static Connection connectPool() throws SQLException, Exception {
        //return connectPool("defaultPool");
        return connectPool("jdbc/cctvoa2ds");
//        return connectPool("java:comp/env/defaultPool"); //tomcat
    }

    /**
     * 数据库连接使用Pool
     */
    public static Connection connectPool(String poolname) throws SQLException, Exception {
        Connection con = null;

        Context ctx = null;

        try {
            DataSource ds = (DataSource) dataSources.get(poolname);
            if (ds == null) {
                ctx = getInitialContext();
                //Logger.debug("ConnectionPool Name:"+Config.getProperty("default_pool"));
                ds = (DataSource) ctx.lookup(poolname);
                dataSources.put(poolname, ds);
                ctx.close();
            }

            con = ds.getConnection();
            //con.setAutoCommit(true);
        } catch (SQLException sqe) {
            logger.debug("DAO.connectPool() error. " + sqe.getMessage());
            sqe.printStackTrace();
            throw sqe;
        } catch (NamingException ne) {
            logger.debug("DAO.connectPool() error. " + ne.getMessage());
            ne.printStackTrace();
            throw new SQLException("connectPool() Naming error:" + ne.getMessage());
        } catch (Exception e) {
            throw e;
        }
        return con;
    }

    /**
     * 直接使用JDBC连接数据库
     */
    public static Connection connectJDBC() throws SQLException {
        Connection conn = null;
        getDBProp();
        try {
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException(" error occured in getConnection() ");
        }
        return conn;
    }


    /**
     * 获得缺省的连接。数据库连结的信息从配置文件中获得。<br>
     *
     * @return Connection 数据库连结。
     */
    public static Connection getConnection() throws SQLException {

        //获取连结
        Connection conn = null;
        try {
            conn = connectPool();
            usePool = true;
        } catch (Exception ex) {
            conn = connectJDBC();
        }
        return conn;
    }

    public static Connection getConnection(String poolName) throws SQLException {

        //获取连结
        Connection conn = null;
        try {
            conn = connectPool(poolName);
            usePool = true;
        } catch (Exception ex) {
            //conn = connectJDBC(poolName);
            logger.debug("没有连接上连接池：" + poolName);
            ex.printStackTrace();
        }
        return conn;
    }


    public static final String escapeQuoteChars(String s) {
        if (s == null)
            return null;
        StringBuffer buf = new StringBuffer(s);
        int i = 0;
        for (int n = buf.length(); i < n; i++)
            if (buf.charAt(i) == '\'') {
                buf.replace(i, i + 1, "''");
                n++;
                i++;
            }

        return buf.toString();
    }


}