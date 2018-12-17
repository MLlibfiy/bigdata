package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseTest {

    HBaseAdmin admin;

    /**
     * 执行Test之前执行的代码块
     */
    @Before
    public void init() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        try {
            /**
             * 创建admin对象，和HMaster建立连接，执行创建，删除，修改表的操作
             *
             */
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void createTable() {
        try {
            //创建表的描述对象
            HTableDescriptor tableDescriptor = new HTableDescriptor("student");
            //增加列簇
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
            //设置列簇的版本数量
            columnDescriptor.setMaxVersions(5);
            tableDescriptor.addFamily(columnDescriptor);
            //判断表是否存在
            if (admin.tableExists("student")) {
                System.out.println("表已存在");
                return;
            }
            //执行建表操作
            admin.createTable(tableDescriptor);

            //判断表是否存在
            if (admin.tableExists("student")) {
                System.out.println("建表成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void deleteTable() {
        try {

            //获取表的描述信息
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("student"));
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                //获取列簇的名称
                //Bytes  hbase提供的把字节数组转换成具体类型的工具
                String name = Bytes.toString(columnFamily.getName());
                System.out.println("列簇名：" + name);
                int maxVersions = columnFamily.getMaxVersions();
                System.out.println("版本数：" + maxVersions);
            }

            /**
             * 删除表之前先进行disable
             *
             */
            admin.disableTable("student");
            //删除表
            admin.deleteTable("student");

            if (!admin.tableExists("student")) {
                System.out.println("表以删除");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 最后执行
     */

    @After
    public void close() {
        System.out.println("回收资源");
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
