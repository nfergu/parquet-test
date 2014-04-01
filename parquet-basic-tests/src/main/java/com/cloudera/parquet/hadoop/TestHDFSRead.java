package com.cloudera.parquet.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.FileReader;
import java.io.IOException;

public class TestHDFSRead extends Configured implements Tool {

    private static final Log LOG = Log.getLog(TestHDFSRead.class);

    private static long total = 0;

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestHDFSRead(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {

        if (args.length < 1) {
            LOG.error("Usage: " + getClass().getName() + " inputFileHDFS");
            return 1;
        }

        String inputFile = args[0];

        Configuration configuration = getConf();
        Job job = new Job(configuration);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputFile));

        job.waitForCompletion(true);

        LOG.info("Output: " + total);

        return 0;

    }

    public static class ReadRequestMap extends Mapper<LongWritable, Text, Void, Void> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String textValue = value.toString();
            if (textValue != null && textValue.trim().length() > 0) {
                total += Integer.parseInt(textValue);
            }
        }
    }

}
