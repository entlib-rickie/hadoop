package com.rickie.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    // hbase 配置
    private static Configuration configuration;
    // hbase 连接
    private static Connection connection;

    private HBaseConn(){
        try{
            if(connection == null) {
                // 使用 HBaseConfiguration 的单例方法实例化
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", "192.168.56.103:2181");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取HBase连接
     * @return
     */
    private Connection getConnection() {
        if(connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return connection;
    }

    /**
     * 获取HBase连接
     * @return
     */
    public static Connection getHBaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     * 获取HBase Table 对象
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if(connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
