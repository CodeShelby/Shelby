package com.tiduyun.dmp.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HbaseConnection  {
    public static Connection createConnection() throws IOException {
        //连接hbase配置
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "192.168.0.96");
        hbaseConfig.set("hbase.zookeeper.property.clientPort","2181");
        hbaseConfig.set("bulk.flush.interval.ms","1000");


        //创建hbase连接
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
        return connection;
    }
}
