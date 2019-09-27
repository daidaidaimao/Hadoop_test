package cn.daimao.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
//        定义变量计算次数
        long num = 0 ;
//        循环遍历集合，进行累加的操作，得出当前单词出现的总次数
        for (LongWritable val:values){
            num += val.get();
        }
//        输出数据  key是单词，value是次数
        context.write(key,new LongWritable(num));
    }
}
