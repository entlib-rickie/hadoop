package com.rickie.sample;

import com.rickie.hbase.api.HBaseConn;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PreSplitRegion {
    public static void main(String[] args) throws IOException {
        org.apache.hadoop.hbase.client.Connection conn = HBaseConn.getHBaseConn();
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("split_table");
        String columnFamily = "basic";
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("TABLE DELETED.");
        }

        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(columnFamily))
                .setBlocksize(32 * 1024)
                .setCompressionType(Compression.Algorithm.SNAPPY)
                .setDataBlockEncoding(DataBlockEncoding.NONE)
                .setTimeToLive(3*60*60*24) // 过期时间
                .setMaxVersions(3);  // 版本数
        tableDescBuilder.setColumnFamily(columnDescBuilder.build());

        byte[][] splitKeys = {
                Bytes.toBytes("10"),
                Bytes.toBytes("100"),
                Bytes.toBytes("1000"),
                Bytes.toBytes("10000"),
                Bytes.toBytes("100000")
        };
        // 创建表
        admin.createTable(tableDescBuilder.build(), splitKeys);
        admin.close();
        conn.close();
    }
}
