package aldwxutil;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则表达式 规则使用
 */
public class regex_rule implements Serializable {
    //关于 app_key 的正则表达式
    /**<br>gcs:<br>查看string类型是否是32位，并且是字母，或者是数字
     *
     * */
    public Boolean chenkak(String string){
        String regex = "[a-z0-9]{32}";
        Pattern compile = Pattern.compile(regex);
        //匹配数据
        Matcher matcher = compile.matcher(string);
        return matcher.matches();
    }

    /**<br>gcs:<br>
     * 判断String类型str是不是里面都是数字
     * */
    public Boolean isNumdr(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(str).matches();
    }

    //关于 qr_key 的正则表达式
    public Boolean chenkqr(String string){
        String regex = "[a-z0-9]{2,32}";
        Pattern compile = Pattern.compile(regex);
        //匹配数据
        Matcher matcher = compile.matcher(string);
        return matcher.matches();
    }



    public static void main(String[] args) {
        String a="a23";
        regex_rule r= new regex_rule();
        System.out.println(r.chenkqr(a));
    }
}
