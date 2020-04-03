package com.rickie;

import static org.junit.Assert.assertTrue;

import com.rickie.hbase.api.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for HBaseUtil
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void createTableTest() {
        HBaseUtil.createTable("student", new String[] {"score"});
        //HBaseUtil.createTable("student2", new String[] {"score", "basic"});
    }

    @Test
    public void putRowTest() {
        HBaseUtil.putRow("student", "Rickie", "score", "Chinese", "90");
        HBaseUtil.putRow("student", "Rickie", "score", "Math", "900");
        HBaseUtil.putRow("student", "Rickie", "score", "Java", "100");
        //HBaseUtil.putRow("student", "Rickie", "basic", "age", "18");
    }

    @Test
    public void putRowsTest() {
        List<Put> puts = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("Jack"));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("Chinese"),Bytes.toBytes("88"));
        puts.add(put);
        put = new Put(Bytes.toBytes("Jack"));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("Math"),Bytes.toBytes("60"));
        puts.add(put);

        HBaseUtil.putRows("student", puts);
    }

    @Test
    public void getRowTest() {
        Result result = HBaseUtil.getRow("student", "rickie");
        System.out.println("查询单行");
        if(result != null && result.rawCells().length >0) {
            Cell[] cells = result.rawCells();
            for(Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("row: " + row + "\t" + family + ":" +column + "\t" + value);
            }
        } else {
            System.out.println("空结果 ... ");
        }
    }

    @Test
    public void getScannerTest() {
        ResultScanner results = HBaseUtil.getScanner("student");
        if(results != null) {
            results.forEach(result -> {
                System.out.println("rowKey = " + Bytes.toString(result.getRow()));
                System.out.println("score:Chinese = " + Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("Chinese"))));
                System.out.println("score:Math = " + Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("Math"))));
                System.out.println("score:Java = " + Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("Java"))));
            });
        }
    }

    @Test
    public void deleteRowTest() {
        HBaseUtil.deleteRow("student", "Rickie");
    }

    @Test
    public void deleteColumnFamilyTest() {
        HBaseUtil.deleteColumnFamily("student", "score");
    }

    @Test
    public void deleteQualifierTest() {
        HBaseUtil.deleteQualifier("student", "Jack","score", "Math");
    }

    @Test
    public void deleteTableTest() {
        HBaseUtil.deleteTable("student");
    }

}
