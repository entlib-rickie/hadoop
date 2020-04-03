package com.rickie;

import com.rickie.wordcount.SumReducer;
import com.rickie.wordcount.TokenizerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * MapReduce word count
 *
 */
public class WordCountApp
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println( "Hello World!" );
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if(otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        } else {
            for (int i = 0; i < otherArgs.length; i++) {
                System.out.println(otherArgs[i]);
            }
        }

        // //通过Configuration对象获取job对象,设置环境参数
        Job job = Job.getInstance(conf, "wordcount");
        // 设置整个应用的类名
        job.setJarByClass(WordCountApp.class);
        // 添加Mapper 类
        job.setMapperClass(TokenizerMapper.class);
        // 添加Reducer 类
        job.setReducerClass(SumReducer.class);
        // 设置key 输出类型
        job.setOutputKeyClass(Text.class);
        // 设置value 输出类型
        job.setOutputValueClass(IntWritable.class);

        for(int i=1; i<otherArgs.length - 1; ++i) {
            // 设置输入文件
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // 设置输出文件
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length -1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
