package freebase_parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import test.testRe;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 16:10 2019/2/28
 * @Modified By: 把对照表上已经有的实体先提取出来，并把关系是freebase网址的数据截取到“/”位置
 * 特殊情况：在reduce阶段，可能会有文件最后生成的相同key值的文件过多，会引起GC错误，这时候需要对这部分数据进行其他的处理 详见step2
 *
 *map的key是<http://rdf.freebase.com/ns/m.0100kt2c>
 *     value是<http://dbpedia.org/resource/Mine_Güngör>
    <http://dbpedia.org/resource/Mine_Güngör> <http://rdf.freebase.com/ns/m.0100kt2c>
 */
public class step1 {
    public static class extractFreebaseMap extends Mapper<Object, Text, Text, Text>{
        Text keyOut = new Text();
        Text valueOut = new Text();
//        Map<String, String> freebaseDBPediaMap;
        testRe testre = new testRe();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            // 读到的是map映射表
            if (info.length == 2){
                testre.setMyURL(String.valueOf(info[1]));
                String temp1 = testre.ParseURL();
                testre.setMyURL(String.valueOf(info[0]));
                String temp0 = testre.ParseURL();
                keyOut.set(temp1); //key
                valueOut.set(temp0); //value
                System.err.println(keyOut + " " + valueOut);

            }
            if (info.length == 4){
                testre.setMyURL(String.valueOf(info[0]));
                String temp0 = testre.ParseURL();
                keyOut.set(temp0);
                testre.setMyURL(String.valueOf(info[1]));
                String temp1 = testre.ParseURL();
                testre.setMyURL(String.valueOf(info[2]));
                String temp2 = testre.ParseURL();
                valueOut.set(temp1 + "\t" + temp2);
                System.err.println("----------------------");
                System.err.println(keyOut + " " + valueOut);

            }
            context.write(keyOut, valueOut);
        }
    }


    public static class extractFreebaseReuce extends Reducer<Text, Text, Text, NullWritable>{
        Text keyOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int flag1 = 0; //标记是否有
            int flag2 = 0;
            int result = 0;
            ArrayList<String> datalist = new ArrayList<>();
            // 判断map中是否找到
            for (Text value : values) {
                datalist.add(value.toString());
            }
            for (String value :datalist){
                if (value.split("\t").length == 2){
                    flag1 = 1;
                }
                if (value.split("\t").length == 1){
                    flag2 = 1;
                }
                if (flag1 == 1 && flag2 == 1){
                    result = 1;  //map映射成功
                }
            }
            System.err.println(flag1 + "+flag+" + flag2);
            System.err.println(result);
            if (result == 1) {
                String map_id = "";
                for (String value : datalist) {
//                    System.err.println(value);
                    String[] info = value.split("\t");
                    if (info.length == 1) {
                        map_id = String.valueOf(info[0]);
                        System.err.println("mapid=" + map_id);
                        break;
                    }
                }
                for (String value : datalist) {
                    String[] info = value.split("\t");
                    if (info.length == 2) {
                        keyOut.set(key.toString() + "\t" + map_id + "\t" + info[0] + "\t" + info[1]);
                        context.write(keyOut, NullWritable.get());
                    }
                }
            }

        }
    }


}
