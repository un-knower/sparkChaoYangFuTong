package com.ald.stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DB {
    private final Logger log = LoggerFactory.getLogger(getClass());
    public boolean operateDB(Connection conn, String sql) {
//        log.debug("=====================================operateDB--------------------------------------");
        boolean flag = true;
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();
        }
        catch (Exception e) {
            flag = false;
            log.error("",e);
        }finally{
            try {
                if(pstmt!=null){
                    pstmt.close();
                }
            } catch (SQLException e) {
                pstmt = null ;
                log.error("",e);
            }
        }
        return flag;
    }

//    public static String

      public int countTableRecord(Connection conn, String sql) {
//        log.debug("=====================================operateDB--------------------------------------");
        boolean flag = true;
        PreparedStatement pstmt = null;
        ResultSet rs = null ;
        int count = 0 ;
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();

            if(rs.next()){
                count = rs.getInt(1) ;
            }

        }
        catch (Exception e) {
            flag = false;
            log.error("",e);
        }finally{
            try {
                if(pstmt!=null){
                    pstmt.close();
                }
            } catch (SQLException e) {
                pstmt = null ;
                log.error("",e);
            }
        }
        return count;
    }
    public ResultSet  getResult(Connection conn, String sql) {
//        log.debug("=====================================operateDB--------------------------------------");
        boolean flag = true;
        PreparedStatement pstmt = null;
        ResultSet rs = null ;
        int count = 0 ;
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();

        }
        catch (Exception e) {
            flag = false;
            log.error("",e);
        }finally{
            try {
                if(pstmt!=null){
                    pstmt.close();
                }
            } catch (SQLException e) {
                pstmt = null ;
                log.error("",e);
            }
        }
        return rs;
    }


}
