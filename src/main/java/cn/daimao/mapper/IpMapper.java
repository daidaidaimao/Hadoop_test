package cn.daimao.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IpMapper extends Mapper<Object, Text,Text,NullWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        line 每一行的ip拿到
//        Text line = value ;
        context.write(value, NullWritable.get());
    }
}
