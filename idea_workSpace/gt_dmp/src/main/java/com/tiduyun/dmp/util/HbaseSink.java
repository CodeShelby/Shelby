package com.tiduyun.dmp.util;

import com.tiduyun.dmp.bean.pub.JsonBean;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseSink {
    public static void writeToHbase (Connection connection,  JsonBean record) {

        try {
            //todo 1.连接hbase的表
            //获取操作对象
            Admin admin = connection.getAdmin();
            //获取指定命名空间下的表
            admin.listTableNamesByNamespace("nc5");
            String tableName;

            //todo 2.解析kafka的数据 插入hbase
            //对数据来源进行判断
            String ds = record.getSchema().getName();
            if (ds.contains("nc5")){
                String tn = record.getPayload().getTABLE_NAME();
                //对表名进行判断
                switch (tn){
                    case "BD_ACCSUBJ":
                        tableName = "BD_ACCSUBJ";
                        String pk_accsubj = record.getPayload().getData().getPK_ACCSUBJ() == null ? " " : record.getPayload().getData().getPK_ACCSUBJ();
                        String subjcode = record.getPayload().getData().getSUBJCODE() == null ? " " : record.getPayload().getData().getSUBJCODE();
                        String subjname = record.getPayload().getData().getSUBJNAME() == null ? " " : record.getPayload().getData().getSUBJNAME();
                        String engsubjname = record.getPayload().getData().getENGSUBJNAME() == null ? " " : record.getPayload().getData().getENGSUBJNAME();
                        String dispname = record.getPayload().getData().getDISPNAME() == null ? " " : record.getPayload().getData().getDISPNAME();
                        String ctlsystem = record.getPayload().getData().getCTLSYSTEM() == null ? " " : record.getPayload().getData().getCTLSYSTEM();
                        String endflag = record.getPayload().getData().getENDFLAG() == null ? " " : record.getPayload().getData().getENDFLAG();
                        String accremove = record.getPayload().getData().getACCREMOVE() == null ? " " : record.getPayload().getData().getACCREMOVE();
                        String cashbankflag = record.getPayload().getData().getCASHBANKFLAG() == null ? " " : record.getPayload().getData().getCASHBANKFLAG();
                        String subjlev = record.getPayload().getData().getSUBJLEV() == null ? " " : record.getPayload().getData().getSUBJLEV();
                        String balanorient = record.getPayload().getData().getBALANORIENT() == null ? " " : record.getPayload().getData().getBALANORIENT();
                        String year = record.getPayload().getData().getCREATEYEAR() == null ? " " : record.getPayload().getData().getCREATEYEAR();
                        String month = record.getPayload().getData().getCREATEPERIOD() == null ? " " : record.getPayload().getData().getCREATEPERIOD();
                        String createTime = year + "-" + month;
                        String modifytime = record.getPayload().getData().getTS() == null ? " " : record.getPayload().getData().getTS();
                        String datasource = "nc5";

                        //创建一张表
                        TableName tb = TableName.valueOf(datasource,tableName);
                        HTableDescriptor htable = new HTableDescriptor(tb);
                        //表添加列簇  最大版本个数为3
                        htable.addFamily(new HColumnDescriptor("f").setMaxVersions(3));
                        //连接表
                        Table table = connection.getTable(tb);
                        //指定rowkey
                        Put put = new Put(Bytes.toBytes(Md5Util.getMD5Str(pk_accsubj).substring(0,8) + datasource));
                        //参数分别为列簇，列限定符，值
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("pk_accsubj"),Bytes.toBytes(pk_accsubj));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("subjcode"),Bytes.toBytes(subjcode));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("subjname"),Bytes.toBytes(subjname));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("engsubjname"),Bytes.toBytes(engsubjname));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("dispname"),Bytes.toBytes(dispname));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("ctlsystem"),Bytes.toBytes(ctlsystem));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("endflag"),Bytes.toBytes(endflag));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("accremove"),Bytes.toBytes(accremove));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("cashbankflag"),Bytes.toBytes(cashbankflag));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("subjlev"),Bytes.toBytes(subjlev));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("balanorient"),Bytes.toBytes(balanorient));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("createtime"),Bytes.toBytes(createTime));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("modifytime"),Bytes.toBytes(modifytime));
                        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("datasource"),Bytes.toBytes(datasource));
                        //最终执行插入
                        table.put(put);
                        table.close();

                        break;

                    case "aaa ":
                        break;
                    case "aaa":
                        break;
                    case " aaa":
                        break;


                }
            } else {

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
