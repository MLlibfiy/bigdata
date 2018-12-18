package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * 通过mapreduce 生产hfile文件
 */
public class HdfsToFile {

    public static class HdfsToHfileMappper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(line[0].getBytes());

            KeyValue name = new KeyValue(line[0].getBytes(), "info".getBytes(), "name".getBytes(), System.currentTimeMillis(), line[1].getBytes());
            KeyValue age = new KeyValue(line[0].getBytes(), "info".getBytes(), "age".getBytes(), System.currentTimeMillis(), line[2].getBytes());


            context.write(rowkey, name);
            context.write(rowkey, age);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf);
        job.setJarByClass(HdfsToFile.class);
        job.setJobName("HdfsToHfie");

        job.setMapperClass(HdfsToHfileMappper.class);
        //指定数据key的类型是hbase rowkey的类型
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        //hbase提供的用来做排序的reduce  字典顺序
        job.setReducerClass(KeyValueSortReducer.class);
        job.setNumReduceTasks(4);
        //设置自定义分区器，保证整体有序
        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        HTable table = new HTable(conf, "student");

        HFileOutputFormat.configureIncrementalLoad(job, table);

        FileInputFormat.addInputPath(job, new Path("/data/student"));

        Path inputPath = new Path("/data/hfile/student");
        FileSystem fileSystem = FileSystem.newInstance(conf);
        if(fileSystem.exists(inputPath)){
            fileSystem.delete(inputPath);
        }

        FileOutputFormat.setOutputPath(job, inputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
