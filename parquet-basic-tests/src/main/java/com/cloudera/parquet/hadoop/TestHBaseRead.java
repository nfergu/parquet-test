package com.cloudera.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
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

import java.io.IOException;

public class TestHBaseRead extends Configured implements Tool {

    private static final Log LOG = Log.getLog(TestHBaseRead.class);

    private static long total = 0;

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestHBaseRead(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {

        if (args.length < 1) {
            LOG.error("Usage: " + getClass().getName() + " inputTableHBase zooKeeperQuorum");
            return 1;
        }

        String inputTable = args[0];
        String zooKeeperQuorum = args[1];

        Configuration configuration = getConf();
        configuration.set("hbase.zookeeper.quorum", zooKeeperQuorum);

        Scan scan = new Scan();
        scan.setCaching(10000);

        Job job = new Job(configuration);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        TableMapReduceUtil.initTableMapperJob(
                inputTable,        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                ReadRequestMap.class,   // mapper
                null,             // mapper output key
                null,             // mapper output value
                job);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.waitForCompletion(true);

        LOG.info("Output: " + total);

        return 0;

    }

    public static class ReadRequestMap extends TableMapper<Void, Void> {
        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            int number = Bytes.toInt(value.value());
            total += number;
        }
    }

}
