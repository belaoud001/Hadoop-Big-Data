package org.guru.Tp.partie3.Question3_23;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.guru.Tp.enumeration.Country;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Question3_23 {

    public static class TopTagsFlickrByCountryMapper1 extends Mapper<LongWritable, Text, Text, StringAndInt> {

        private boolean validFields(String... fields) {
            return Arrays.stream(fields)
                         .noneMatch(field -> field == null || field.isEmpty());
        }

        private Country getCountry(String longitude, String latitude) {
            if (validFields(longitude, latitude))
                return Country.getCountryAt(Double.parseDouble(latitude), Double.parseDouble(longitude));
            return null;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] flickrRecord = value.toString().split("\t");
            String longitude = flickrRecord[10];
            String latitude	 = flickrRecord[11];

            Country country = getCountry(longitude, latitude);

            if (country != null) {
                String decodedUserTags    = URLDecoder.decode(flickrRecord[8],"UTF-8");
                String decodedMachineTags = URLDecoder.decode(flickrRecord[9],"UTF-8");
                String joinedTags = decodedUserTags + "," + decodedMachineTags;

                for (String tag : joinedTags.split(","))
                    context.write(new Text(country.toString()), new StringAndInt(tag));
            }
        }

    }

    public static class TopTagsFlickrByCountryCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
        @Override
        protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> tagFrequencyMap = new HashMap<>();

            for (StringAndInt value : values)
                tagFrequencyMap.merge(value.tag, 1, Integer::sum);

            for (Map.Entry<String, Integer> entry : tagFrequencyMap.entrySet())
                context.write(key, new StringAndInt(entry.getKey(), entry.getValue()));
        }
    }

    public static class TopTagsFlickrByCountryReducer1 extends Reducer<Text, StringAndInt, Text, StringAndInt> {
        @Override
        protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> tagFrequencyMap = new HashMap<>();

            for (StringAndInt value : values)
                tagFrequencyMap.merge(value.tag, value.occurrence, Integer::sum);

            for (Map.Entry<String, Integer> entry : tagFrequencyMap.entrySet())
                context.write(key, new StringAndInt(entry.getKey(), entry.getValue()));
        }
    }

    public static class TopTagsFlickrByCountryMapper2 extends Mapper<Text, StringAndInt, StringAndInt, StringAndInt> {
        @Override
        protected void map(Text key, StringAndInt value, Context context) throws IOException, InterruptedException {
            context.write(new StringAndInt(key.toString(), value.tag, value.occurrence), value);
        }
    }

    public static class TopTagsFlickrByCountryComparator extends WritableComparator {

        protected TopTagsFlickrByCountryComparator(){
            super(StringAndInt.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StringAndInt comparable1 = (StringAndInt) a;
            StringAndInt comparable2 = (StringAndInt) b;
            int compareResult = comparable1.country.compareTo(comparable2.country);

            return compareResult == 0 ? comparable1.compareTo(comparable2) : compareResult;
        }

    }

    public static class TopTagsFlickrByCountryGroupingComparator extends WritableComparator {

        protected TopTagsFlickrByCountryGroupingComparator(){
            super(StringAndInt.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StringAndInt comparable1 = (StringAndInt) a;
            StringAndInt comparable2 = (StringAndInt) b;

            return comparable1.country.compareTo(comparable2.country);
        }

    }

    public static class TopTagsFlickrByCountryReducer2 extends Reducer<StringAndInt, StringAndInt, Text, Text> {
        @Override
        protected void reduce(StringAndInt key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            int numbOfTags = context.getConfiguration()
                                    .getInt("K", 1);

            for (StringAndInt value : values) {
                context.write(new Text(key.country), new Text(value.toString()));

                if (--numbOfTags == 0) break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input = otherArgs[0];
        String output = otherArgs[1];
        conf.setInt("K", Integer.parseInt(otherArgs[2]));

        Job job1 = Job.getInstance(conf, "Question3_23");
        job1.setJarByClass(Question3_23.class);

        job1.setMapperClass(TopTagsFlickrByCountryMapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(StringAndInt.class);

        job1.setReducerClass(TopTagsFlickrByCountryReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(StringAndInt.class);

        job1.setCombinerClass(TopTagsFlickrByCountryCombiner.class);

        FileInputFormat.addInputPath(job1, new Path(input));
        job1.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Question3_23");
        job2.setJarByClass(Question3_23.class);

        job2.setMapperClass(TopTagsFlickrByCountryMapper2.class);
        job2.setMapOutputKeyClass(StringAndInt.class);
        job2.setMapOutputValueClass(StringAndInt.class);

        job2.setSortComparatorClass(TopTagsFlickrByCountryComparator.class);
        job2.setGroupingComparatorClass(TopTagsFlickrByCountryGroupingComparator.class);

        job2.setReducerClass(TopTagsFlickrByCountryReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(output));
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        FileOutputFormat.setOutputPath(job2, new Path(output + "/job2_output"));
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.waitForCompletion(true);
    }

}
