package freebase_parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 10:34 2019/3/7
 * @Modified By: 将type.object.type提取出来
 */
public class step5 {
    public static class GetObjectMap extends Mapper<Object, Text, Text, NullWritable>{
        Text keyOut = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            if (info[3].equals("type.object.type")){
                keyOut.set(value);
                context.write(keyOut, NullWritable.get());
            }

        }
    }
}
