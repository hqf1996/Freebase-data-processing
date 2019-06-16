package freebase_to_neo4j_format;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 9:53 2019/3/17
 * @Modified By:
 */
public class format_duplicate {
    public static class FormatMap extends Mapper<Object, Text, Text, Text>{
        Text keyOut = new Text();
        Text valueOut = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String info = value.toString();
            String newinfo = new String(info.getBytes(),"UTF-8");
            keyOut.set(newinfo);
            valueOut.set(newinfo);
            context.write(keyOut, valueOut);
        }
    }

    public static class FormatReduce extends Reducer<Text, Text, Text, NullWritable>{
        Text keyout =  new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                String newinfo = new String(value.toString().getBytes(),"UTF-8");
                keyout.set(newinfo+ "," + "\"" + "Entity" + "\"");
                context.write(keyout, NullWritable.get());
                break;
            }
        }
    }
}
