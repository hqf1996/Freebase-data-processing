package freebase_parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 11:19 2019/3/7
 * @Modified By:autology表中的属性部分需要把相同属性的标上同一个ID
 *              autology表的表结构为：实体ID----属性ID[之前用了UUID，全是不同的]----实体----type.object.type----类型属性
 */
public class step6 {
    public static class mergeIDSameMap extends Mapper<Object, Text, Text, Text>{
        Text keyOut = new Text();
        Text valueOut = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            if (info.length == 5){
                keyOut.set(info[4]);
                valueOut.set(value);
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class mergeIDSameReduce extends Reducer<Text, Text, Text, NullWritable>{
        public int i = 0;
        Text keyOut = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                String []info = value.toString().split("\t");
                keyOut.set(info[0] + "\t" + i + "\t" + info[2] + "\t" + info[3] + "\t" + info[4]);
                context.write(keyOut, NullWritable.get());
            }
            i++;
        }
    }
}
