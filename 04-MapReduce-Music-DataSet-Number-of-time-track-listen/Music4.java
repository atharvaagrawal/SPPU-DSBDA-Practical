import java.io.*;
//import java.util.*;

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


public class Music4 {
	public static void main(String[] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path inputfile=new Path(files[0]),outputfile=new Path(files[1]);
		Job j1=new Job(c,"getlisteners");
		j1.setJarByClass(Music4.class);
		j1.setMapperClass(Music1Mapper.class);
		j1.setReducerClass(Music1Reducer.class);
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(j1,outputfile);
		FileInputFormat.addInputPath(j1,inputfile);
		System.exit(j1.waitForCompletion(true)?0:1);
	}
	public static class Music1Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		private static final IntWritable RADIO = new IntWritable(1);
		private static final IntWritable SKIP = new IntWritable(2);
		private Text trackId = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] fields = line.split("\t");
			if(fields.length >= 5){
				trackId.set(fields[1]);
				int radio = Integer.parseInt(fields[3]);
				int skip = Integer.parseInt(fields[4]);
				if(radio == 1)
				{
					context.write(trackId, RADIO);
				}
				if(skip == 1)
				{
					context.write(trackId, SKIP);
				}
			}
		}
	}
	public static class Music1Reducer extends Reducer<Text,IntWritable,Text,Text> {
		//int count=0;
		private final Text result = new Text();
		public void reduce(Text key, Iterable<IntWritable> args,Context con) throws IOException, InterruptedException{
			//++count;
			int radioCount = 0;
            int skipCount = 0;

            for (IntWritable value : args) {
                int count = value.get();
                if (count == 1) {
                    radioCount++;
                } else if (count == 2) {
                    skipCount++;
                }
            }
            
            String output = "Track ID: " + key.toString() + ", Listened on Radio: " + radioCount + ", Skipped: " + skipCount;
            result.set(output);
            con.write(key, result);

		}
	}
}
