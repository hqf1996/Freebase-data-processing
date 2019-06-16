package freebase_parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import test.testReId;
import util.Util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 9:42 2019/3/6
 * @Modified By: 对宾语进行映射，并增添ID，即表中的第4个字段，映射规则为：当能够在映射表中找到的时候，将名称提取出来，
 * 新增为第二个字段，当成其ID。当在映射表中找不到的时候，分为两种情况，当映射表表中的数据是m.aa1af1这种格式的，舍弃这条
 * 数据，如果是另外的数据格式则保留，其ID可以编成UUID的形式，使每一个都不重复。
 */
public class step3 {
    public static class AddIDMap extends Mapper<Object, Text, Text, NullWritable>{
        Text keyOut = new Text();
        Text valueOut = new Text();
        testReId testid = new testReId();
        Map<String, String> freebaseDBPediaMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            freebaseDBPediaMap = Util.getMapFromDir("freebase_links_en_simply.ttl", "\t", 1, 0);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            if (info.length == 4) {
                //没有在映射表中找到
                if (freebaseDBPediaMap.get(info[3]) == null) {
                    testid.setInputStr(info[3]);
                    if (!testid.ParseIDTrue()) {
                        String uuid = UUID.randomUUID().toString();
                        keyOut.set(info[0] + "\t" + uuid + "\t" + info[1] + "\t" + info[2] + "\t" + info[3]);
                        context.write(keyOut, NullWritable.get());
                    }
                } else if (freebaseDBPediaMap.get(info[3]) != null) {  //在映射表中找到了
                    keyOut.set(info[0] + "\t" + info[3] + "\t" + info[1] + "\t" + info[2] + "\t" + freebaseDBPediaMap.get(info[3]));
                    context.write(keyOut, NullWritable.get());
                }
            }
            else {
                System.err.println(Arrays.toString(info));
            }
        }
    }

//    public static class AddIDReduce extends Reducer<Text, Text, Text, NullWritable>{
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//        }
//    }
}
