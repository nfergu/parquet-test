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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.Log;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.FileReader;
import java.io.IOException;

public class TestHDFSWrite extends Configured implements Tool {

    private static final Log LOG = Log.getLog(TestHDFSWrite.class);

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestHDFSWrite(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            LOG.error("Usage: " + getClass().getName() + " inputFileHDFS outputFileHDFS");
            return 1;
        }

        String inputFile = args[0];
        String outputFile = args[1] + System.nanoTime();

        Configuration configuration = getConf();
        Job job = new Job(configuration);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.waitForCompletion(true);

        return 0;

    }

    public static class ReadRequestMap extends Mapper<LongWritable, Text, Void, Text> {
        private MessageType schema = null;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(null, value);
        }
    }

}
