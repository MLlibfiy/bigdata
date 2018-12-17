package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class HbaseToHdfs {

    public static class HbaseToHdfsMap extends TableMapper<NullWritable, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String age = "";
            String name = "";
            String gender = "";
            String clazz = "";

            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                if ("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    age = Bytes.toString(CellUtil.cloneValue(cell));
                } else if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    name = Bytes.toString(CellUtil.cloneValue(cell));
                } else if ("gender".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    gender = Bytes.toString(CellUtil.cloneValue(cell));
                } else if ("clazz".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    clazz = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }

            String line = Bytes.toString(key.get()) + "\t" + name + "\t" + age + "\t" + gender + "\t" + clazz;
            context.write(NullWritable.get(), new Text(line));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(5);
        job.setJarByClass(HbaseToHdfs.class);
        job.setJobName("HbaseToHdfs");


        Scan scan = new Scan();
        scan.addFamily("info".getBytes());
        TableMapReduceUtil.initTableMapperJob("student", scan, HbaseToHdfsMap.class, NullWritable.class, Text.class, job);


        FileOutputFormat.setOutputPath(job, new Path("/data/out/student"));


        boolean b = job.waitForCompletion(true);


    }
}
