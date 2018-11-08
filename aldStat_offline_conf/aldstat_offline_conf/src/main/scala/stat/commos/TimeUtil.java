package stat.commos;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhangyanpeng on 2017/7/27.
 */
public class TimeUtil implements Serializable {

    public static String time2st(String time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        Long timestap=new Date().getTime();
        try {
           timestap= sdf.parse(time).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timestap.toString();
    }
    //将 st 时间转换成 年月日 类型
    public static String st2Day(Long longtime){
        String day = toDay(longtime).substring(0, 10);
        return day;
    }
    //将 st 时间转换成 hour类型
    public static String st2hour(Long longtime){

        String hours = toDay(longtime).substring(11, 13);
        return hours;
    }
    public static String toDay(Long longtime){
        Date date=new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String d = sdf.format(longtime);
        try {
            date=sdf.parse(d);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return d;
    }


    //判断传入的参数是不是日期格式(传参数)   默认今天
    public static String processArgs(String[] args){
        String date="";
        for (int i=0 ;i<args.length;i++){
            if ("-d".equals(args[i])){
                if (i+1 < args.length){
                    date=args[i+1];
                }
            }
        }
        if (StringUtils.isNotBlank(date) && chenkDate(date)){
            String[] stDate = date.split("-");
            return stDate[0]+stDate[1]+stDate[2];
        }else {
            String tTime = tTime();
            return tTime;
        }
    }

    //判断传入的参数是不是日期格式(传参数)  默认昨天
    public static String processArgs2(String[] args){
        String date="";
        for (int i=0 ;i<args.length;i++){
            if ("-d".equals(args[i])){
                if (i+1 < args.length){
                    date=args[i+1];
                }
            }
        }
        if (StringUtils.isNotBlank(date) && chenkDate(date)){
            String[] stDate = date.split("-");
            return stDate[0]+stDate[1]+stDate[2];
        }else {
            String tTime = yTime();
            return tTime;
        }
    }

    //判断 日期格式
    public static Boolean chenkDate(String date){
        String regex = "[2][0][0-9]{2}-[0-9]{2}-[0-9]{2}";
        Pattern compile = Pattern.compile(regex);
        //匹配数据
        Matcher matcher = compile.matcher(date);
        return matcher.matches();
    }
    //获得昨天的日期
    public static String yTime(){
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String s = sdf.format(calendar.getTime());
        return s;
    }

    //获得今天的日期
    public static String tTime(){
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String s = sdf.format(calendar.getTime());
        return s;
    }

    //获得今天的 当前小时的前一个小时
    public static String getHour(){
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String hours= dateFormat.format(now.getTime()-3600000);
        return hours.substring(11, 13);
    }

    public static String processArgsHour(String[] args){
        String hour="";
        for (int i=0 ;i<args.length;i++){
            if ("-h".equals(args[i])){
                if (i+1 < args.length){
                    hour=args[i+1];
                }
            }
        }
        if (StringUtils.isNotBlank(hour) && chenkHour(hour)){
            return hour;
        }else {
            String Mhour = getHour();
            return Mhour;
        }
    }

    public static Boolean chenkHour(String hour){
        String regex = "[0-9]{2}";
        Pattern compile = Pattern.compile(regex);
        //匹配数据
        Matcher matcher = compile.matcher(hour);
        return matcher.matches();
    }

}

