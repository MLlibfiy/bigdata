package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 插入数据的同时往索引表里面插入一行数据
 *
 * 对班级 和 性别 增加二级索引
 *
 */
public class HbaseIndexJava {

    @Test
    public void insert() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");

        HConnection connection = null;

        HTableInterface studentInterface = null;
        HTableInterface studentIndexInterface = null;
        //创建zookeeper连接
        try {
            connection = HConnectionManager.createConnection(conf);
            //数据表
            studentInterface = connection.getTable("student");
            //索引表
            studentIndexInterface = connection.getTable("student_index");
            //加载磁盘上的数据
            ArrayList<Student> students = DataUtil.load("E:\\bigdata\\bigdata\\data\\students.txt", Student.class);
            for (Student student : students) {
                //先查入索引表   rowkey: 班级_性别_rowkey
                String indexRowkey = student.getClazz() + "_" + student.getGender() + "_" + student.getId();
                Put indexPut = new Put(indexRowkey.getBytes());
                //虽然插入了数据，实际上是用不到的，只需要rowkey就够了
                indexPut.add("f".getBytes(),"q".getBytes(),"|".getBytes());
                studentIndexInterface.put(indexPut);

                //插入数据
                byte[] rowkey = student.getId().getBytes();
                Put put = new Put(rowkey);
                put.add("info".getBytes(), "age".getBytes(), Bytes.toBytes(student.getAge()));
                put.add("info".getBytes(), "name".getBytes(), Bytes.toBytes(student.getName()));
                put.add("info".getBytes(), "clazz".getBytes(), Bytes.toBytes(student.getClazz()));
                put.add("info".getBytes(), "gender".getBytes(), Bytes.toBytes(student.getGender()));
                studentInterface.put(put);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (studentInterface!=null){
                try {
                    studentInterface.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (studentIndexInterface!=null){
                try {
                    studentIndexInterface.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 查询
     * 先查索引表
     * 根据索引表得到的rowkey查询数据表
     *
     */
    @Test
    public void query(){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");

        HConnection connection = null;

        HTableInterface studentInterface = null;
        HTableInterface studentIndexInterface = null;
        //创建zookeeper连接
        try {
            connection = HConnectionManager.createConnection(conf);
            //数据表
            studentInterface = connection.getTable("student");
            //索引表
            studentIndexInterface = connection.getTable("student_index");


            /**
             * 查询文科一班的所有学生
             *
             * 查询索引表
             *
             * 使用rowkey前缀过滤器
             */

            BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator("文科一班_男".getBytes());
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, binaryPrefixComparator);
            Scan scan = new Scan();
            scan.setFilter(rowFilter);
            ResultScanner scanner = studentIndexInterface.getScanner(scan);
            Result next;



            ArrayList<Get> gets = new ArrayList<>();

            while ((next = scanner.next())!=null){
                // 科目_性别_id
                String indexRowkey = Bytes.toString(next.getRow());
                String id = indexRowkey.split("_")[2];

                Get get = new Get(id.getBytes());
                gets.add(get);
            }


            //根据索引表获取的rowkey查询数据表
            Result[] results = studentInterface.get(gets);

            for (Result result : results) {
                List<Cell> cells = result.listCells();
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
        }finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (studentInterface!=null){
                try {
                    studentInterface.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (studentIndexInterface!=null){
                try {
                    studentIndexInterface.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }

}
