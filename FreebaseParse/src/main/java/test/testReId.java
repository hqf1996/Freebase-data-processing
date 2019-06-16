package test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 10:20 2019/3/6
 * @Modified By: 正则匹配是否是m.js21kk这种类型
 */
public class testReId {
    public String inputStr;

    public void setInputStr(String inputStr) {
        this.inputStr = inputStr;
    }

    public boolean ParseIDTrue(){
        String pattern = "[a-zA-Z]{1}\\.[A-Za-z0-9\\_]+";
        Pattern p = Pattern.compile(pattern);

        Matcher m = p.matcher(inputStr);
        if (m.find()){
//            System.out.println("匹配到的是" + m.group(0));
            if (m.group(0).equals(inputStr)){
                return true;
            }
            else
                return false;
        }
        else
            return false;
    }

    public static void main(String[] args) {
        testReId test = new testReId();
//        test.setInputStr("aam.aaaa122_");
//        test.setInputStr("base.type_ontology.non_agent");
        test.setInputStr("m.04m8");
        System.out.println(test.ParseIDTrue());
    }
}
