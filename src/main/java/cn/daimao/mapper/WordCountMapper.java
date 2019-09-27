package cn.daimao.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text, Text,LongWritable> {

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line =value.toString();
        String[] arr = line.split(" ");
        for (String str : arr){
            context.write(new Text(str),new LongWritable(1));
        }
    }
}
