package com.tiduyun.dmp.dao;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTable {
    static Put put;

    public HbaseTable() {
    }

    public HbaseTable(String rowkey)
    {
        put = new Put(Bytes.toBytes(rowkey));
    }

    public static Table getTable(Connection connection, String datasource, String table) throws IOException {
        //获取操作对象
        Admin admin = connection.getAdmin();
        //创建一张表
        TableName tb = TableName.valueOf(datasource,table);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tb);
        //表添加列簇  最大版本个数为3
        hTableDescriptor.addFamily(new HColumnDescriptor("f").setMaxVersions(3));
        //连接表
        Table htable = connection.getTable(tb);

        return htable;
    }

    //往hbase插入数据
    public static void putCell(Table table,Put put,String columnFamily,String column, String value) throws IOException {
        //指定rowkey
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
    }

    public static void closeTable(Table table) throws IOException {
        table.close();
    }

}
