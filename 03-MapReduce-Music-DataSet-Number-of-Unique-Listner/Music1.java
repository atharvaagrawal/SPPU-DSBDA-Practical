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


public class Music1 {
	public static void main(String[] args) throws Exception{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path inputfile=new Path(files[0]),outputfile=new Path(files[1]);
		Job j1=new Job(c,"getlisteners");
		j1.setJarByClass(Music1.class);
		j1.setMapperClass(Music1Mapper.class);
		j1.setReducerClass(Music1Reducer.class);
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(j1,outputfile);
		FileInputFormat.addInputPath(j1,inputfile);
		System.exit(j1.waitForCompletion(true)?0:1);
	}
	public static class Music1Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		public void map(LongWritable key,Text textfile,Context con) throws IOException, InterruptedException{
			String textlines=textfile.toString();
			String[] values=textlines.split("\t");
			if (values.length >= 5) {
				Text songid=new Text(values[1]);
				IntWritable shared=new IntWritable(Integer.parseInt(values[3]));
				con.write(songid, shared);
			}
		}
	}
	public static class Music1Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		//int count=0;
		public void reduce(Text key, Iterable<IntWritable> args,Context con) throws IOException, InterruptedException{
			//++count;
			int count=0;
			int sharedcount=0;
			for(IntWritable value: args){
				++count;
				sharedcount+=value.get();
			}
			
			String uniquelistener="Number of unique listeners for track id "+key.toString()+" :";
			//uniquelistener.concat(key.toString());
			//uniquelistener.concat(": ");
			String shares="Number of times track id "+key.toString()+" was shared: ";
			//shares.concat(key.toString());
			con.write(new Text(uniquelistener),new IntWritable(count));
			con.write(new Text(shares),new IntWritable(sharedcount));
		}
	}
}
