package freebase_parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Util;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 11:14 2018/9/14
 * @Modified By:
 */
public class step2main {
    public static String fs = "hdfs://10.1.13.111:8020";//hdfs://10.1.18.221:8020   psd:hdu52335335
    public static Logger LOG = LoggerFactory.getLogger(step2main.class);
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String currentDate = Util.getLocalData();
//        String pathHead = fs + "/user/freebase/result";
//        HdfsUtil.createDir(conf, pathHead, LOG);
        String []otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Usage:wordcount <in><out>");
            System.exit(2);
        }
        conf.set("mapred.jar", "F:\\JavaProjects\\DataOperate\\target\\hadooptest2-1.0-SNAPSHOT-2.0.jar");
//        conf.set("mapreduce.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "step2_parse");
        job.setJarByClass(step2.class);
        job.setMapperClass(step2.SimplifyStepMap.class);

        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
