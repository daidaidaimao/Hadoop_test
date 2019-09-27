package cn.daimao.driver;

import cn.daimao.mapper.IpMapper;
import cn.daimao.reducer.IpReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IpDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        Job job  = Job.getInstance(conf);

        job.setJarByClass(IpDriver.class);

        job.setMapperClass(IpMapper.class);
        job.setReducerClass(IpReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //        设置输入路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://118.31.103.189:9000/txt/txt/ip.txt"));
//        设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://118.31.103.189:9000/result/IpResult"));

        if (!job.waitForCompletion(true)){
            return;
        }
    }
}
