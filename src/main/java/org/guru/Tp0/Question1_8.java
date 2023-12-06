package org.guru.Tp0;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Question1_8 {

    public enum CustomCounter {
        EMPTY_LINE;
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Map<String, Integer> inMapperCombiner;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            inMapperCombiner = new HashMap<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if ("".equals(value.toString())) {
                context.getCounter(CustomCounter.EMPTY_LINE).increment(1);
                return;
            }

            for (String stringLoop : value.toString().split(" ")) {
                stringLoop = stringLoop.replaceAll("\\s*,\\s*$", "");
                stringLoop = stringLoop.trim();

                inMapperCombiner.merge(stringLoop, 1, Integer::sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : inMapperCombiner.entrySet())
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));

            super.cleanup(context);
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input = otherArgs[0];
        String output = otherArgs[1];

        Job job = Job.getInstance(conf, "Question1_8");
        job.setJarByClass(Question1_8.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);

        Counters counters = job.getCounters();
        org.apache.hadoop.mapreduce.Counter c1 = counters.findCounter(CustomCounter.EMPTY_LINE);
        System.out.println(c1.getDisplayName() + " : " + c1.getValue());

        System.exit(0);
    }

}
