package com.hadoop.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;


public class HdfsApi {
//    public static final String HDFS_PATH = "hdfs://hadoop000:9000";
    public static final String HDFS_PATH = "hdfs://192.168.111.128:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Test
    public void mkdir()throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void create()throws Exception{
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/1.txt"));
        fsDataOutputStream.write("hello nihao".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    @Test
    public void cat() throws Exception{
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/hello.txt"));
        IOUtils.copyBytes(fsDataInputStream,System.out,1024);
        fsDataInputStream.close();
    }

    @Before
    public void setUp() throws Exception{
        System.out.println("# start");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"cdl");

    }

    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem.close();
        System.out.println("# end");
    }
}
