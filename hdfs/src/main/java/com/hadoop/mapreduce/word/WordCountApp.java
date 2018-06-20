package com.hadoop.mapreduce.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountApp {
    /**
     *  Map:读取输入的文件
     *
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
//            super.map(key,value,context);
            LongWritable one = new LongWritable(1);
            String line = value.toString();
            String[] words =  line.split(" ");
            for(String word : words){
                //通过上下文把map的处理结果输出
                context.write(new Text(word),one);
            }
        }
    }

    /**
     * Reduce:归并操作
     */
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException,InterruptedException{

            long sum = 0;
            for(LongWritable value : values){
                sum += value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }

    /**
     * 定义Driver
     */
    public static void main(String[] args )throws IOException,InterruptedException,ClassNotFoundException{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"wordCount");
        job.setJarByClass(WordCountApp.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.out.println("执行结束");

//        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
