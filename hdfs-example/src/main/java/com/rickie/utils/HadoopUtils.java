package com.rickie.utils;

import org.apache.hadoop.conf.Configuration;

public class HadoopUtils {
    public static Configuration hadoopConf() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", "hdfs://192.168.56.103:9000");
        conf.set("dfs.replication", "1");

        return conf;
    }
}
