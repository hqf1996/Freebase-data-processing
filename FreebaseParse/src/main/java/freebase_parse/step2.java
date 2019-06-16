package freebase_parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import test.testRe;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 19:56 2019/3/5
 * @Modified By:step1中部分之前方法生成的文件是全部url格式的，但是后面的输出文件有加入了简化策略。这里主要针对之前的全部URL的数据进行简化
 *  输入是四个字段都没有简化过的，这边对四个字段都进行简化
 */
public class step2 {
    public static class SimplifyStepMap extends Mapper<Object, Text, Text, NullWritable>{
        testRe testre = new testRe();
        Text keyout = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []info = value.toString().split("\t");
            testre.setMyURL(info[0]); //主语对应的Key
            String temp0 = testre.ParseURL();
            testre.setMyURL(info[1]); //主语
            String temp1 = testre.ParseURL();
            testre.setMyURL(info[2]);  //谓词
            String temp2 = testre.ParseURL();
            testre.setMyURL(info[3]);  //宾语
            String temp3 = testre.ParseURL();
            keyout.set(temp0 + "\t" + temp1 + "\t" + temp2 + "\t" + temp3);
            context.write(keyout, NullWritable.get());
        }
    }
}
