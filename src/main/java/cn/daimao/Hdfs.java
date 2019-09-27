package cn.daimao;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Hdfs {

    @Test
    public void getFile() throws URISyntaxException, IOException {
        Configuration conf=new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://118.31.103.189:9000"),conf);
        InputStream in = fs.open(new Path("/result/ScoreMaxResult2/part-r-00000"));
        OutputStream out = new FileOutputStream("part-r-00000");
        IOUtils.copyBytes(in,out,conf);
    }

//    创建文件夹
    @Test
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://118.31.103.189:9000"),conf,"root");
        fs.mkdirs(new Path("/fromJava"));

    }

//    查询目录文件
    @Test
    public void search() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://118.31.103.189:9000"),conf,"root");
        FileStatus[] ls = fs.listStatus(new Path("/"));
        for(FileStatus status:ls) {
            System.out.println(status);
            System.out.println("wdnmd");
        }
    }

//    递归查看指定目录下的文件
    @Test
    public void getFileFrom() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://118.31.103.189:9000"),conf,"root");
        RemoteIterator<LocatedFileStatus> rt = fs.listFiles(new Path("/"),true);
        while (rt.hasNext()){
            System.out.println(rt.next());
        }
    }

//    上传文件
    @Test
    public void upload() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("dfs.client.use.datanode.hostname","true");
//        conf.set("fs.defaultFS", "hdfs://izbp1grkell15a8b3053b3z:8020");
        conf.set("dfs.replication","1");
        FileSystem fs = FileSystem.get(new URI("hdfs://118.31.103.189:9000"),conf,"root");
        ByteArrayInputStream in = new ByteArrayInputStream("hello hdfs".getBytes());
        OutputStream out = fs.create(new Path("/fromJava/test6.txt"));
        IOUtils.copyBytes(in,out,conf);
        System.out.println("成功！");
    }

//    删除
    @Test
    public void delete() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://118.31.103.189:9000"),conf,"root");
        fs.delete(new Path("/fromJava/test.txt"),true);
        fs.close();
    }
}
