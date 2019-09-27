package cn.daimao.driver;

import cn.daimao.mapper.CharacterMapper;
import cn.daimao.reducer.CharacterCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CharacterDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        Job job  = Job.getInstance(conf);

        job.setJarByClass(CharacterDriver.class);
        job.setMapperClass(CharacterMapper.class);
        job.setReducerClass(CharacterCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://118.31.103.189:9000/txt/txt/characters.txt"));
//        设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://118.31.103.189:9000/result/charactersResult4"));

        if (!job.waitForCompletion(true)){
            return;
        }
    }
}
