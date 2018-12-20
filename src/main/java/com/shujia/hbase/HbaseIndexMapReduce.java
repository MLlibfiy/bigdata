package com.shujia.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * 对已经存在的表建立索引
 */
public class HbaseIndexMapReduce {

    public static class HbaseIndexMap extends TableMapper<Text, NullWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {





            String gender = "";
            String clazz = "";

            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                if ("gender".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    gender = Bytes.toString(CellUtil.cloneValue(cell));
                } else if ("clazz".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    clazz = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }

            String line = ContentUtil.getClazzIndex(clazz) + "_" + ContentUtil.getGenderIndex(gender) + "_" + Bytes.toString(key.get());
            context.write(new Text(line), NullWritable.get());
        }
    }

    public static class HbaseIndexReducer extends TableReducer<Text, NullWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            String rowkey = key.toString();
            Put put = new Put(rowkey.getBytes());
            put.add("f".getBytes(),"q".getBytes(),"|".getBytes());
            context.write(NullWritable.get(),put);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.out.println("开始建立索引");

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        Job job = Job.getInstance(conf);
        job.setJarByClass(HbaseIndexMapReduce.class);
        job.setJobName("HbaseIndexMapReduce");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        Scan scan = new Scan();
        scan.addFamily("info".getBytes());

        //指定map
        TableMapReduceUtil.initTableMapperJob("student", scan,HbaseIndexMap.class, Text.class, NullWritable.class, job);

        //指定reduce
        TableMapReduceUtil.initTableReducerJob("student_index",HbaseIndexReducer.class, job,null,null,null,null,false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        boolean b = job.waitForCompletion(true);


    }
}