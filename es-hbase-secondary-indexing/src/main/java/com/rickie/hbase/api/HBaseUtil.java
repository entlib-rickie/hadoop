package com.rickie.hbase.api;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBaseUtil {
    private static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    /**
     * 创建表
     * @param tableName
     * @param cfs
     * @return
     */
    public static boolean createTable(String tableName, String[] cfs) {
        try(HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if(admin.tableExists(TableName.valueOf(tableName))) {
                logger.info("Table {} exists.", tableName);
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
            logger.info("Table {} created.", tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * 删除表
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String tableName) {
        try(HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if(!admin.tableExists(TableName.valueOf(tableName))) {
                logger.info("Table {} does not exist.", tableName);
                return false;
            }
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            logger.info("Table {} deleted.", tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * 插入数据
     * @param tableName
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @param data
     * @return
     */
    public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier, String data) {
        try(Table table = HBaseConn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 批量插入数据
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try(Table table = HBaseConn.getTable(tableName)) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * 查询单条数据
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Result getRow(String tableName, String rowKey) {
        try(Table table = HBaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<Result> getRows(String tableName, List<String> lstRowkey) {
        List<Result> results = new ArrayList<>();

        try {
            Table table = HBaseConn.getTable(tableName);
            for(String rowkey : lstRowkey) {
                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                results.add(result);
            }
            return results;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * scan扫描数据
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName) {
        try(Table table = HBaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            return results;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除行
     * @param tableName
     * @param rowKey
     * @return
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try(Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * 删除列族
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteColumnFamily(String tableName, String cfName) {
        try(HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            admin.deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(cfName));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 删除列
     * @param tableName
     * @param rowKey
     * @param cfName
     * @param qualifierName
     * @return
     */
    public static boolean deleteQualifier(String tableName, String rowKey, String cfName, String qualifierName) {
        try(Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifierName));
            table.delete(delete);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
