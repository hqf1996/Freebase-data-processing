package freebase_parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 16:40 2019/3/5
 * @Modified By:对部分无法进行数据处理的数据进行处理，将data文件中能够映射到实体的key，拿到映射map中比对，取出map文件中的
 * key value映射表，生成一个新的映射表
 */
public class step1_copy {
    public static class getKeyValueMap extends Mapper<Object, Text, Text, NullWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String info[] = value.toString().split("\t");
        }
    }
}
