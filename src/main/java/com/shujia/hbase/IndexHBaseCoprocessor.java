package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
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
