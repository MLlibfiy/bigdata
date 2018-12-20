package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseTest {

    private HBaseAdmin admin;
    HConnection connection;

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

            //创建zookeeper连接
            connection = HConnectionManager.createConnection(conf);
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
     * 插入一条数据
     */
    @Test
    public void insert() {
        HTableInterface table = null;
        try {
            //获取表对象
            table = connection.getTable("student");
            //创建put对象指向rowkey
            Put put = new Put("001".getBytes());
            put.add("info".getBytes(), "name".getBytes(), "张三".getBytes());
            put.add("info".getBytes(), "age".getBytes(), Bytes.toBytes(23));
            put.add("info".getBytes(), "clazz".getBytes(), "文科一班".getBytes());
            put.add("info".getBytes(), "clazz".getBytes(), "文科一班".getBytes());
            put.add("info".getBytes(), "gender".getBytes(), "男".getBytes());

            //插入一行数据数据
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 一次插入多行数据
     */
    @Test
    public void insertAll() {

        HTableInterface table = null;
        try {
            table = connection.getTable("student");
            //加载磁盘上的数据
            ArrayList<Student> students = DataUtil.load("E:\\bigdata\\bigdata\\data\\students.txt", Student.class);

            ArrayList<Put> puts = new ArrayList<Put>();

            for (Student student : students) {
                byte[] rowkey = student.getId().getBytes();

                Put put = new Put(rowkey);
                put.add("info".getBytes(), "age".getBytes(), Bytes.toBytes(student.getAge()));
                put.add("info".getBytes(), "name".getBytes(), Bytes.toBytes(student.getName()));
                put.add("info".getBytes(), "clazz".getBytes(), Bytes.toBytes(student.getClazz()));
                put.add("info".getBytes(), "gender".getBytes(), Bytes.toBytes(student.getGender()));
                puts.add(put);
            }

            //插入多行数据
            table.put(puts);


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    @Test
    public void query() {
        HTableInterface table = null;
        try {
            table = connection.getTable("student");
            Get get = new Get("1500100007".getBytes());
            //执行查询返回结果
            Result result = table.get(get);
            //获取所有列
            List<Cell> cells = result.listCells();

            //遍历每一个单元格
            for (Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));

                if ("age".equals(qualifier)) {
                    Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                    System.out.println(qualifier + ":" + value);
                } else {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(qualifier + ":" + value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Test
    public void scanner() {
        HTableInterface table = null;
        try {
            table = connection.getTable("student");

            //创建扫描器对象
            Scan scan = new Scan();

            //指定列簇扫描
            scan.addFamily("info".getBytes());

            //执行送秒操作，返回多行结果
            ResultScanner results = table.getScanner(scan);

            Result line;
            //迭代结果集合
            while ((line = results.next()) != null) {
                //获取这一行的所有列
                List<Cell> cells = line.listCells();
                //遍历每一个单元格
                for (Cell cell : cells) {

                    //获取rowkey
                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                    System.out.print(rowkey+"\t");
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if ("age".equals(qualifier)) {
                        Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                        System.out.print(qualifier + ":" + value + "\t");
                    } else {
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.print(qualifier + ":" + value + "\t");
                    }
                }

                System.out.println();
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Test
    /**
     * 查询文科班的所有学生
     *
     */
    public void queryWenke(){
        HTableInterface table = null;
        try {
            table = connection.getTable("student");

            //创建扫描器对象
            Scan scan = new Scan();

            //指定列簇扫描
            scan.addFamily("info".getBytes());


            //增加过滤器，过滤文科班的学生
            RegexStringComparator regexStringComparator = new RegexStringComparator("文科");
            SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter("info".getBytes(), "clazz".getBytes(), CompareFilter.CompareOp.EQUAL, regexStringComparator);

            SubstringComparator comp = new SubstringComparator("男");
            SingleColumnValueFilter columnValueFilter1 = new SingleColumnValueFilter("info".getBytes(), "gender".getBytes(), CompareFilter.CompareOp.EQUAL, comp);

            //过滤器集合
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            //前缀过滤
            BinaryPrefixComparator prefixComparator = new BinaryPrefixComparator(Bytes.toBytes("吕"));

            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),CompareFilter.CompareOp.EQUAL, prefixComparator);

            //过滤文科班的学生
            fl.addFilter(columnValueFilter);

            //过滤性别为男的学生
            fl.addFilter(columnValueFilter1);

            //增加前缀过滤
            fl.addFilter(filter);

            scan.setFilter(fl);
            //执行送秒操作，返回多行结果
            ResultScanner results = table.getScanner(scan);

            Result line;
            //迭代结果集合
            while ((line = results.next()) != null) {
                //获取这一行的所有列
                List<Cell> cells = line.listCells();
                //遍历每一个单元格
                for (Cell cell : cells) {
                    //获取rowkey
                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                    System.out.print(rowkey+"\t");
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if ("age".equals(qualifier)) {
                        Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                        System.out.print(qualifier + ":" + value + "\t");
                    } else {
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.print(qualifier + ":" + value + "\t");
                    }
                }
                System.out.println();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 行键过滤器
     *
     */
    @Test
    public void rowFilter() {
        HTableInterface table = null;
        try {
            table = connection.getTable("student");

            //创建扫描器对象
            Scan scan = new Scan();

            //指定列簇扫描
            scan.addFamily("info".getBytes());


            /**
             * 设置开始key 和结束key
             *
             */
            /*scan.setStartRow("1500100199".getBytes());
            scan.setStopRow("1500100253".getBytes());*/


            /**
             * rowkey前缀过滤
             *
             */
            BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator("15001002".getBytes());
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, binaryPrefixComparator);

            scan.setFilter(rowFilter);

            //执行送秒操作，返回多行结果
            ResultScanner results = table.getScanner(scan);

            Result line;
            //迭代结果集合
            while ((line = results.next()) != null) {
                //获取这一行的所有列
                List<Cell> cells = line.listCells();
                //遍历每一个单元格
                for (Cell cell : cells) {
                    //获取rowkey
                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                    System.out.print(rowkey + "\t");
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if ("age".equals(qualifier)) {
                        Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                        System.out.print(qualifier + ":" + value + "\t");
                    } else {
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.print(qualifier + ":" + value + "\t");
                    }
                }
                System.out.println();
            }

        }catch (Exception e){
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

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
