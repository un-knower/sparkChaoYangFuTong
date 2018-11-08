package aldwxutil;

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

    //将 st 时间转换成 年月日 类型
    /**<br>gcs:<br>
     * 先把一个long类型的时间yyyy-mm-dd 00:00:00 的格式转换为这样的yyyy-... 的时间格式。之后将转换成功的时间格式的day 提取出来
     * @param longtime 要进行时间转换的long类型的时间
     * @return  将Day返回去的格式是yyyy-MM-dd的格式
     * */
    public String st2Day(Long longtime){
        TimeUtil t = new TimeUtil();
        String day = t.toDay(longtime).substring(0, 10);
        return day;
    }
    //将 st 时间转换成 hour类型
    //将 st 时间转换成 hour类型
    public String st2hour(Long longtime){
        TimeUtil t = new TimeUtil();
        String hours = t.toDay(longtime).substring(11, 13);
        return hours;
    }

    /**<br>gcs:<br>
     *将一个long类型的时间longtime先转换为 "yyyy-MM-dd HH:mm:ss" 的时间格式
     * 之后将格式转换成功的"yyyy-MM-dd HH:mm:ss"的时间
     * @param longtime 要进行时间转换的long类型的时间
     * @return 返回将longtime转换成功后的格式为"yyyy-MM-dd HH:mm:ss"的时间
     * */
    public String toDay(Long longtime){
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


    //
    /**<br>gcs:<br>
     * 判断传入的参数是不是日期格式(传参数)   默认获得今天的时间
     * @param args 从-d的参数判断传进来的参数中是否包含-d参数。如果包含-d的参数就把后面的时间提取出来
     * @return 返回"20180625" 这个的时间的格式的String类型的时间
     * */
    public String processArgs(String[] args){

        String date="";
        for (int i=0 ;i<args.length;i++){
            if ("-d".equals(args[i])){ //gcs:如果传进来的args的参数中，有-d这个参数，此时就要把-d后面的时间参数提取出来
                if (i+1 < args.length){
                    date=args[i+1];
                }
            }
        }
        //gcs:判断date是否为Blank，以及查看时间的格式
        if (StringUtils.isNotBlank(date) && chenkDate(date)){
            String[] stDate = date.split("-"); //gcs:将时间按照"-"进行分隔开
            return stDate[0]+stDate[1]+stDate[2]; //gcs:然后将类似于"2018-06-25"的时间，变成"20180625"的时间格式
        }else {
            String tTime = tTime(); //gcs:如果发现date是不符合时间的格式的，此时就会把当前的时间tTime返回来
            return tTime;
        }
    }

    //判断传入的参数是不是日期格式(传参数)  默认昨天
    /**<br>gcs:<br>
     * 将jar包中的args当中的-d 2018-06-25 后面的时间参数2018-06-25 提取出来
     * 如果程序当中没有-d 这个参数，就默认返回"昨天的日期"
     * */
    public String processArgs2(String[] args){
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
            String tTime = yTime(); //gcs:获得昨天的日期
            return tTime;
        }
    }

    //
    /**<br>gcs:<br>
     * 判断 日期格式是否符合2018-03-23的时间格式
     * @param date 要用来判断格式的String类型的时间
     * @return 如果date符合时间的格式，就会返回来true，否则会返回来false
     * */
    public Boolean chenkDate(String date){
        String regex = "[2][0][0-9]{2}-[0-9]{2}-[0-9]{2}";
        Pattern compile = Pattern.compile(regex);
        //匹配数据
        Matcher matcher = compile.matcher(date);
        return matcher.matches();
    }
    //获得昨天的日期
    /**<br>gcs:<br>
     * 获得昨天的日期
     * */
    public String yTime(){
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String s = sdf.format(calendar.getTime());
        return s;
    }

    //获得今天的日期
    public String tTime(){
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String s = sdf.format(calendar.getTime());
        return s;
    }
    //获得今天的日期
    public String nowTime(){
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = sdf.format(calendar.getTime());
        return s;
    }
    //获得今天的 当前小时的前一个小时
    public String getHour(){
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String hours= dateFormat.format(now.getTime()-3600000);
        return hours.substring(11, 13);
    }

    /**<br>gcs:<br>
     * 把args当中的参数当中的-h 后面的小时的参数提取出来。如果发现-h后面没有参数，此时就会取得当前的小时的时间
     * @param args 从当期那的参数中把-h后面的小时的参数提取出来
     * @return 将String类型的小时参数返回回来
     * */
    public String processArgsHour(String[] args){
        String hour="";
        for (int i=0 ;i<args.length;i++){
            if ("-h".equals(args[i])){
                if (i+1 < args.length){
                    hour=args[i+1];
                }
            }
        }
        //gcs:如果hour的参数为空
        if (StringUtils.isNotBlank(hour) && chenkHour(hour)){
            return hour; //gcs:如果hour符合格式，此时就会返回String类型的时间
        }else {
            String Mhour = getHour();
            return Mhour; //gcs:否则返回当前的时间
        }
    }

    /**<br>gcs:<br>
     *使用正则表达式，判断小时 05 是否是两位的数字。
     * @param hour 用来判断是否符合格式的hour
     * @return 如果给的hour符合正则表达式，此时就会返回true，否则就会返回false
     * */
    public Boolean chenkHour(String hour){
        String regex = "[0-9]{2}";
        Pattern compile = Pattern.compile(regex);
        //匹配数据
        Matcher matcher = compile.matcher(hour);
        return matcher.matches();
    }


    public static void main(String[] args) {
        TimeUtil t = new TimeUtil();

        //System.out.println(t.chenkak(a));
    }
}
