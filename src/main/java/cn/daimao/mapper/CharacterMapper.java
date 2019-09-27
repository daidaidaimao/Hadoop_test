package cn.daimao.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CharacterMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split(" ");
        for(String str : arr){
            String regEx="[a-zA-Z0-9]";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(str);
            StringBuffer sb = new StringBuffer();
            while (m.find()){
                sb.append(m.group());
            }
            context.write(new Text(new String(sb)),new LongWritable(1));

        }

    }
    @Test
    public void zhengze(){
        String str = "travel,";
//        String regEx="[a-zA-Z0-9\\u4e00-\\u9fa5]";
        String regEx="[a-zA-Z0-9]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        StringBuffer sb = new StringBuffer();
            while(m.find()){
                sb.append(m.group());
        }
        System.out.println(sb);

    }
}
