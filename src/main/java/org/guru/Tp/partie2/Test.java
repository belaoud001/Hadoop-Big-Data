package org.guru.Tp.partie2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Test {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(";");
            String produit = record[1];
            String pays = record[6];
            String client = record[4];

            context.write(new Text(client), new Text(pays));
        }

    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max   = context.getConfiguration().getInt("max_occurence", Integer.MIN_VALUE);
            int count = 0;
            Set<String> pays = new HashSet<>();

            for (Text value : values) {
                pays.add(value.toString());
                count++;
            }

            if (max < count) {
                context.getConfiguration().setStrings("pays", pays.toString());
                context.getConfiguration().setStrings("client", key.toString());
                context.getConfiguration().setInt("max_occurence", count);
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String client = context.getConfiguration().get("client");
            String pays = context.getConfiguration().get("pays");

            context.write(new Text(client), new Text(pays));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

/*
        conf.setInt("K", Integer.parseInt(otherArgs[2]));
*/

        Job job = Job.getInstance(conf, "Test");

        job.setJarByClass(Test.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
