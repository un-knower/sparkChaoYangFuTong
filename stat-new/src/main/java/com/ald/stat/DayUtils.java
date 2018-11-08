package com.ald.stat;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DayUtils {

    public static String formatDate(Date date, String pattern) {
        String formatDate = "";
        if (pattern != null && pattern.length() > 0) formatDate = DateFormatUtils.format(date, pattern);
        else formatDate = DateFormatUtils.format(date, "yyyyMMdd");
        return formatDate;
    }

    public static String getDateStr(long timestamp) {
//    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.ofHours(8)).format(DateTimeFormatter.BASIC_ISO_DATE);
        Date d = new Date(timestamp);
        return formatDate(d, null);
    }

    public static Long getDayStartTimeStamp(String dateStr) {
        Date date = getDateOfString(dateStr);
        if (date != null) {
            return date.getTime();
        } else {
            return null;
        }
    }

    public static Long getDayAfterTimeStamp(String dateStr) {
        Date date = getDateOfString(dateStr);
        if (date != null) {
            return date.getTime();
        } else {
            return null;
        }
    }

    public static Date getDateOfString(String dateStr) {
        try {
            return DateUtils.parseDate(dateStr, "yyyyMMdd");
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 得到指定日期的前n天
     *
     * @param dayStr
     * @param num
     * @return
     */
    public static String getCalDays(String dayStr, int num) {
        Date date = getDateOfString(dayStr);
        if(date == null) return  null;
        return  formatDate(DateUtils.addDays(date,num),null);

    }


    public static String getCurrStringDays(String dayId) {
        // dayId格式"yyyyMMdd"
        if (dayId == null || dayId.trim().length() != 8) {
            return "";
        }
        return dayId;
    }

    public static String getCurrDateDays(String dayId) {
        SimpleDateFormat sdfD = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date parse = null;
        // dayId格式"yyyyMMdd"
        if (dayId == null || dayId.trim().length() != 8) {
            return null;
        }
        try {
            parse = sdfD.parse(dayId);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String format = sdf.format(parse);
        return format;
    }

    public static void main(String[] args) {

    }
}
