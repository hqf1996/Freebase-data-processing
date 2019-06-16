package test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * @Author: hqf
 * @description:
 * @Data: Create in 20:10 2019/3/4
 * @Modified By: 尖括号的URL被处理，其他情况认为不用被处理
 */
public class testRe {
    public String myURL;

    public testRe() {
    }

    public void setMyURL(String myURL) {
        this.myURL = myURL;
    }

    public String ParseURL(){
        String url_result = "";
        String pattern = "<.*/(.*?)>";
        Pattern p = Pattern.compile(pattern);

        Matcher m = p.matcher(myURL);
        if (m.find()){
//            System.out.println("匹配到的是" + m.group(0));
//            System.out.println("匹配到的是" + m.group(1));
            url_result = m.group(1);
        }
        else {
            url_result = myURL;
        }

        return url_result;
    }

    public static void main(String[] args) {
//        testRe mytest = new testRe("<http://rdf.freebase.com/ns/m.01006d>");
//        testRe mytest = new testRe("<http://dbpedia.org/resource/Oak_Point,_Texas>");
//        testRe mytest = new testRe("<http://rdf.freebase.com/key/wikipedia.sr>");
//        testRe mytest = new testRe("\"Dickens_$0028Texas$0029\"");
//        testRe mytest = new testRe("\"Oak Point\"@nl");
//        String result = mytest.ParseURL();
//        System.out.println(result);

    }
}
