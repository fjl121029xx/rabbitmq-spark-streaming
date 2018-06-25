package com.li.mq.utils;

import org.apache.hadoop.conf.Configuration;

public class HdfsUtil {

    public static final org.apache.hadoop.conf.Configuration Configuration =
            new Configuration();

    static {
        Configuration.set("fs.defaultFS", "hdfs://192.168.100.26:8020/");
        System.setProperty("HADOOP_USER_NAME", "root");
    }
}
