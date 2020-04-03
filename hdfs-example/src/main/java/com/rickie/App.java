package com.rickie;

import com.rickie.utils.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
/**
 * HDFS examples
 * author：rickie
 */
public class App 
{
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main( String[] args ) throws IOException {
        System.out.println( "Hello World!" );
        String remotePath = "hdfs://192.168.56.103:9000/user/root/input/";
        String remoteFile = "hdfs://192.168.56.103:9000/user/root/input/hadoop-quickstart.jpg";
        String localPath = "d:/tmp/hadoop-quickstart.jpg";

        if(!fileExist(remoteFile)) {
            mkdir(remotePath);
            put(localPath, remotePath);
        } else {
            remove(remoteFile);
        }
    }

    /**
     * 检查指定的文件是否存在
     * @param remoteFile
     * @return
     * @throws IOException
     */
    public static boolean fileExist(String remoteFile) throws IOException {
        FileSystem fs = null;
        try {
            String filename = remoteFile;
            Configuration conf = HadoopUtils.hadoopConf();
            fs = FileSystem.get(conf);
            if(fs.exists(new Path(filename))) {
                System.out.println("File exists.");
                return true;
            } else {
                System.out.println("File doesn't exist.");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fs != null) {
                fs.close();
            }
        }
        return false;
    }

    /**
     * 创建目录
     * @param remotePath
     * @throws IOException
     */
    public static void mkdir(String remotePath) throws IOException {
        Configuration conf = HadoopUtils.hadoopConf();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(remotePath);
        if(!fs.exists(path)) {
            fs.mkdirs(path);
        }
        fs.close();
    }

    /**
     * 上传：将本地文件上传至分布式文件系统指定目录
     * @param localPath
     * @param remotePath
     * @throws IOException
     */
    public static void put(String localPath, String remotePath) throws IOException {
        Configuration conf = HadoopUtils.hadoopConf();
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(localPath);
        Path dst = new Path(remotePath);
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    /**
     * 删除文件
     * @param remoteFile
     * @throws IOException
     */
    public static void remove(String remoteFile) throws IOException {
        Configuration conf = HadoopUtils.hadoopConf();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(remoteFile);
        if(fs.exists(path)){
            fs.delete(path, true);
            logger.info("DELETED!");
        }
        fs.close();
    }
}
