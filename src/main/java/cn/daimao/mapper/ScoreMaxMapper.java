package cn.daimao.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMaxMapper extends Mapper<Object, Text,Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name;
        int grade;
        String[] line = value.toString().split("\\s+");
        name = line[0];
        grade = Integer.parseInt(line[1]);
        context.write(new Text(name),new IntWritable(grade));
    }
}
