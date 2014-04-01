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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.Log;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.FileReader;
import java.io.IOException;

public class TestBasicRead extends Configured implements Tool {

    private static final Log LOG = Log.getLog(TestBasicRead.class);

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestBasicRead(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            LOG.error("Usage: " + getClass().getName() + " schemaFileLocal inputDirectoryHDFS");
            return 1;
        }

        String schemaFile = args[0];
        String inputDirectory = args[1];

        FileReader fileReader = new FileReader(schemaFile);
        String schemaContent = IOUtils.toString(fileReader);
        fileReader.close();

        MessageType schema = MessageTypeParser.parseMessageType(schemaContent);

        Configuration configuration = getConf();
        configuration.set("parquetSchema", schemaContent);

        Job job = new Job(configuration);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        ExampleOutputFormat.setSchema(job, schema);

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));

        job.waitForCompletion(true);

        return 0;

    }

    /*
     * Read a Parquet record, write a Parquet record
     */
    public static class ReadRequestMap extends Mapper<LongWritable, Group, Void, Void> {
        private MessageType schema = null;
        @Override
        public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            if (schema == null) {
                Configuration configuration = context.getConfiguration();
                schema = MessageTypeParser.parseMessageType(configuration.get("parquetSchema"));
            }
            LOG.info("Read value [" + value + "]");
        }
    }

}
