package PackageDemo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LogFile {

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job j = new Job(c, "ipcountlog");
        j.setJarByClass(LogFile.class);
        j.setMapperClass(MapForIpCountLog.class);
        j.setReducerClass(ReduceForIpCountLog.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
    public static class MapForIpCountLog extends Mapper < LongWritable, Text, Text, IntWritable > {
        public void map(LongWritable key, Text value, Context con)
        throws IOException,
        InterruptedException {
            String line = value.toString();
            String[] words = line.split("[,\n]");
            for (int i = 0; i < words.length / 3; i += 3) {
                String word = words[i];
                String timestamp1 = words[i + 1];
                String timestamp2 = words[i + 2];
                Text outputKey = new Text(word.trim());
                IntWritable outputValue = new IntWritable(calulate(timestamp1, timestamp2));
                con.write(outputKey, outputValue);
            }
        }
        public int calulate(String timestamp1, String timestamp2) {
            int hour1 = Integer.parseInt(timestamp1.substring(0, 2));
            int minute1 = Integer.parseInt(timestamp1.substring(3, 5));
            int second1 = Integer.parseInt(timestamp1.substring(6, 8));
            int hour2 = Integer.parseInt(timestamp2.substring(0, 2));
            int minute2 = Integer.parseInt(timestamp2.substring(3, 5));
            int second2 = Integer.parseInt(timestamp2.substring(6, 8));
            // calculate the difference between the two timestamps in seconds
            int hours = hour2 - hour1;
            int minutes = minute2 - minute1;
            int seconds = second2 - second1;
            int totalSeconds = (hours * 3600) + (minutes * 60) + seconds;
            // print the duration in seconds
            return totalSeconds / 3600;
        }
    }
    public static class ReduceForIpCountLog extends
    Reducer < Text, IntWritable, Text, IntWritable > {
        int max_sum = 0;
        int mean = 0;
        int count = 0;
        Text max_occured_key = new Text();
        Text count_key = new Text("Count : ");
        int min_sum = Integer.MAX_VALUE;
        Text min_occured_key = new Text();
        public void reduce(Text word, Iterable < IntWritable > values, Context con)
        throws IOException,
        InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
                count++;
            }
            con.write(word, new IntWritable(sum));
            if (sum < min_sum) {
                min_sum = sum;
                min_occured_key.set("Least Frequent Ip Address " + word);
            }
            if (sum > max_sum) {
                max_sum = sum;
                max_occured_key.set("Most Frequent Ip Address " + word);
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,
        InterruptedException {
            context.write(max_occured_key, new IntWritable(max_sum));
            context.write(min_occured_key, new IntWritable(min_sum));
            context.write(count_key, new IntWritable(count));
        }
    }

}