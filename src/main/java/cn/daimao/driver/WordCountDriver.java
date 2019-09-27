package cn.daimao.driver;

import cn.daimao.mapper.WordCountMapper;
import cn.daimao.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        Job job  = Job.getInstance(conf);

//        设置当前程序入口类
        job.setJarByClass(cn.daimao.driver.WordCountDriver.class);

//        设置mapper类
        job.setMapperClass(WordCountMapper.class);
//        设置reducer类
        job.setReducerClass(WordCountReducer.class);

//        设置mapper的结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

//        设置reduce的结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        设置输入路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://118.31.103.189:9000/txt/txt/characters.txt"));
//        设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://118.31.103.189:9000/result/charactersResult2"));

        if (!job.waitForCompletion(true)){
            return;
        }
    }
}
