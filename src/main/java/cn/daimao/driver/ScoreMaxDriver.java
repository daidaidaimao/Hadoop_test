package cn.daimao.driver;

import cn.daimao.mapper.ScoreMaxMapper;
import cn.daimao.reducer.ScoreMaxReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScoreMaxDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        Job job  = Job.getInstance(conf);

        job.setJarByClass(ScoreMaxDriver.class);

        job.setMapperClass(ScoreMaxMapper.class);
        job.setReducerClass(ScoreMaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://118.31.103.189:9000/txt/txt/score2.txt"));
//        设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://118.31.103.189:9000/result/ScoreMaxResult2"));

        if (!job.waitForCompletion(true)){
            return;
        }
    }
}
