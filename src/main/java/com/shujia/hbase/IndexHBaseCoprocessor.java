package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class IndexHBaseCoprocessor extends BaseRegionObserver {
    static HTableInterface table;

    static {
        Configuration conf = HBaseConfiguration.create();

        HConnection connection = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            //数据表
            table = connection.getTable("student_index");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    /**
     *
     * 在对表执行删除操作时执行此方法
     *
     */
    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {

        String row = Bytes.toString(delete.getRow());

        Scan scan = new Scan();

        RegexStringComparator regexStringComparator = new RegexStringComparator(".*" + row);

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, regexStringComparator);

        scan.setFilter(rowFilter);

        ResultScanner scanner = table.getScanner(scan);

        Result next;


        while ((next = scanner.next()) != null) {

            String gender = null;
            String clazz = null;

            List<Cell> cells = next.listCells();
            for (Cell cell : cells) {
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    if ("gender".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        gender = Bytes.toString(CellUtil.cloneValue(cell));
                    } else if ("clazz".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        clazz = Bytes.toString(CellUtil.cloneValue(cell));
                    }
                }
            }
            if(clazz!=null && gender!=null){
                String rowkey = ContentUtil.getClazzIndex(clazz) + "_" + ContentUtil.getGenderIndex(gender);

                //删除索引表里的索引
                table.delete(new Delete(rowkey.getBytes()));
            }
        }
    }

    /**
     * 在往表里面插入数据的时候会执行 prePut
     * 那么我们只需要在这个方法里面往索引表里面插入索引
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {


        String row = Bytes.toString(put.getRow());

        //获取班级
        List<Cell> cells1 = put.get("info".getBytes(), "clazz".getBytes());
        String clazz = Bytes.toString(CellUtil.cloneValue(cells1.get(0)));

        //获取性别
        List<Cell> cells2 = put.get("info".getBytes(), "gender".getBytes());
        String gender = Bytes.toString(CellUtil.cloneValue(cells2.get(0)));


        //构建索引表rowkey
        String rwokey = ContentUtil.getClazzIndex(clazz) + "_" + ContentUtil.getGenderIndex(gender) + "_" + row;


        Put put1 = new Put(rwokey.getBytes());

        put1.add("f".getBytes(), "q".getBytes(), "|".getBytes());

        table.put(put1);

    }
}
