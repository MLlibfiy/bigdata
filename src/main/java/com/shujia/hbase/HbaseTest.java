package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.junit.Test;

import java.io.IOException;

public class HbaseTest {

    @Test
    public void createTable() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        HBaseAdmin admin = null;
        try {
            /**
             * 创建admin对象，和HMaster建立连接，执行创建，删除，修改表的操作
             *
             */
            admin = new HBaseAdmin(conf);

            //创建表的描述对象
            HTableDescriptor tableDescriptor = new HTableDescriptor("student");
            //增加列簇
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
            //设置列簇的版本数量
            columnDescriptor.setMaxVersions(5);
            tableDescriptor.addFamily(columnDescriptor);

            //判断表是否存在
            if (admin.tableExists("student")){
                System.out.println("表已存在");
                return;
            }
            //执行建表操作
            admin.createTable(tableDescriptor);

            //判断表是否存在
            if (admin.tableExists("student")){
                System.out.println("建表成功");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("finally");
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }




}
