package org.guru.Tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.thirdparty.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Question2_2 {

    private static class StringAndInt implements Comparable<StringAndInt>, Writable {

        public String tag;
        public Integer occurence;

        public StringAndInt() {}

        public StringAndInt(String tag) {
            this.tag = tag;
            this.occurence = 1;
        }

        public StringAndInt(String tag, int occurence) {
            this.tag = tag;
            this.occurence = occurence;
        }

        @Override
        public int compareTo(StringAndInt stringAndInt) {
            return occurence.compareTo(occurence);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            tag = dataInput.readUTF();
            occurence = dataInput.readInt();
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(tag);
            dataOutput.writeInt(occurence);
        }

        @Override
        public String toString() {
            return tag + " : " + occurence;
        }

    }

    public static class TopTagsFlickrByCountryMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {

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

    public static class TopTagsFlickrByCountryReducer extends Reducer<Text, StringAndInt, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> tagFrequencyMap = new HashMap<>();

            for (StringAndInt value : values)
                tagFrequencyMap.merge(value.tag, value.occurence, Integer::sum);

            int numberOfTags = context.getConfiguration().getInt("K", 1);


            MinMaxPriorityQueue<StringAndInt> topTagsQueue = MinMaxPriorityQueue.maximumSize(numberOfTags)
                                                                                .create();

            for (Map.Entry<String, Integer> entry : tagFrequencyMap.entrySet())
                topTagsQueue.add(new StringAndInt(entry.getKey(), entry.getValue()));

            for (StringAndInt stringAndInt : topTagsQueue)
                context.write(key, new Text(stringAndInt.toString()));
        }

    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.setInt("K", Integer.parseInt(otherArgs[2]));

        Job job = Job.getInstance(conf, "Question2_2");

        job.setJarByClass(Question2_2.class);
        job.setMapperClass(TopTagsFlickrByCountryMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringAndInt.class);

        job.setReducerClass(TopTagsFlickrByCountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setCombinerClass(TopTagsFlickrByCountryCombiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
