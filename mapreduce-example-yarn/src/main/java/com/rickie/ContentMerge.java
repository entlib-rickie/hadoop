package com.rickie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ContentMerge {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        //定义hadoop数据类型Text实例text，一行数据
        private static Text text = new Text();

        @Override
        //作为map方法输入的键值对，其value值存储的是文本文件中的一行（以回车符为行结束标记），
        // 而key值为该行的首字母相对于文本文件的首地址的偏移量（或文本文件中行号）。
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            text = value;
            //hadoop全局类context输出函数write
            context.write(text, new Text(""));
        }
    }

    //reduce 方法的输入参数 key为单行文本，而 values 是由各Mapper上的空串，不关注。
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //输出结果到hdfs
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.56.103:9000");
        String[] otherArgs = new String[]{"input2", "output2"};
        Job job = Job.getInstance(conf, "Merge and duplicate removal");
        job.setJarByClass(ContentMerge.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
