package freebase_parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 20:43 2019/3/6
 * @Modified By:实体1ID-实体2ID-实体1-谓词-实体2
 * 现在由于数据量过于庞大，将实体1与实体2的ID都是在映射表的字段都提取出来，组成关系
 */
public class step4 {
    public static class SelectIDMap extends Mapper<Object, Text, Text, NullWritable>{
        Text keyout = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            if (info[1].length() != 36){
                context.write(value, NullWritable.get());
            }
        }
    }
}
