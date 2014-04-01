package com.cloudera.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.Log;
import parquet.schema.MessageType;

import java.io.IOException;

public class TestHBaseWrite extends Configured implements Tool {

    private static final Log LOG = Log.getLog(TestHBaseWrite.class);

    private static int keyInt = 0;

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestHBaseWrite(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {

        if (args.length < 3) {
            LOG.error("Usage: " + getClass().getName() + " inputFileHDFS outputTableHBase quorumAddress");
            return 1;
        }

        String inputFile = args[0];
        String outputTable = args[1];
        String quorumAddress = args[2];

        Configuration configuration = getConf();
        configuration.set(TableOutputFormat.QUORUM_ADDRESS, quorumAddress);
        configuration.set(TableOutputFormat.OUTPUT_TABLE, outputTable);

        Job job = new Job(configuration);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputFile));

        job.waitForCompletion(true);

        return 0;

    }

    public static class ReadRequestMap extends Mapper<LongWritable, Text, Void, Put> {
        private MessageType schema = null;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String textValue = value.toString();
            if (textValue != null && textValue.trim().length() > 0) {
                Put put = new Put(Bytes.toBytes(++keyInt));
                put.add(Bytes.toBytes("d"), new byte[]{}, Bytes.toBytes(Integer.parseInt(textValue)));
                context.write(null, put);
            }
        }
    }

}
