package com.ald.stat.cache;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.time.LocalDate;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by zhaofw on 2018/7/31.
 */
public class TestJava {
    static final String baseRedisKey = "rt";

    public static void main(String[] args) {
        System.out.println(getRepireKeyTime());
        String d = getDateStr(getRepireKeyTime());
        String key = "rt" + ":" + d + ":" + "newUser";

        System.out.println(key);

    }

    //设置为次日凌晨2点过期
     public static Long getRepireKeyTime(){
        return LocalDate.now().atTime(2, 0).plusDays(1).atZone(TimeZone.getTimeZone("GMT+0800").toZoneId()).toEpochSecond();
    }


    public static String getDateStr(Long timestamp){
        Date d = new Date(toMillsecond(timestamp));
        return formatDate(d, null);
    }

    public static Long toMillsecond(Long timestamp){
        Long timeLong = timestamp;
        if (timestamp.toString().length() == 10) timeLong = timestamp * 1000;
        return timeLong;
    }

    public static String formatDate(Date date,String  pattern){
        String formatDate = "";
        if (pattern != null && pattern.length() > 0) formatDate = DateFormatUtils.format(date, pattern);
        else formatDate = DateFormatUtils.format(date, "yyyyMMdd");
        return formatDate;
    }


}
