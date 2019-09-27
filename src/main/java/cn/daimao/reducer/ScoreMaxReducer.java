package cn.daimao.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreMaxReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxNum =  0 ;
        for (IntWritable line : values){
            if(line.get()>maxNum){
                maxNum=line.get();
            }
        }
        context.write(key,new IntWritable(maxNum));
    }
}
