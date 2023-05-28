package PS5Package;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Movies {
	 
	 public static void main(String [] args) throws Exception{
         Configuration c = new Configuration();
         String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
         Path input=new Path(files[0]);
         Path output=new Path(files[1]);
         
         Job j=new Job(c,"movies");
         j.setJarByClass(Movies.class);
         j.setMapperClass(MovieMapper.class);
         j.setReducerClass(MovieReducer.class);
         j.setOutputKeyClass(Text.class);
         j.setOutputValueClass(DoubleWritable.class);
         //DistributedCache.addCacheFile(new Path(files[2]).toUri(),c);
         FileInputFormat.addInputPath(j, input);
         FileOutputFormat.setOutputPath(j, output);
         System.exit(j.waitForCompletion(true)?0:1);
	 }

	public static class MovieMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
		
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			 String data=value.toString();
			 String[] lines=data.split("\n");
			 for(String line: lines){
				 String[] cols=line.split(",");
				 String movieid=cols[1],ratingstring=cols[2];
				 con.write(new Text(movieid), new DoubleWritable(Double.parseDouble(ratingstring)));
			 }
		 }
	}
	
	public static class MovieReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		HashMap<String,Double> hmap=new HashMap<>(),sortedHmap=new HashMap<>();
		 public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {
			 double sum=0,count=0;
			 for(DoubleWritable rating: values){
				 double currentRating=rating.get();
				 ++count;
				 sum+=currentRating;
			 }
			 double avg=sum/(count*1.0000);
			 hmap.put(key.toString(), avg);			 
		 }
		 @Override
		 protected void cleanup(Context con) throws IOException,InterruptedException{
			 List<Map.Entry<String,Double>> list=new LinkedList<Map.Entry<String,Double>>(hmap.entrySet());
			
			 Collections.sort(list,new Comparator<Map.Entry<String,Double>>(){
				 public int compare(Map.Entry<String, Double> a,Map.Entry<String,Double> b){
					 return a.getValue()>b.getValue()?1:0;
				 }
			 });
			 hmap.clear();
			 for(Map.Entry<String, Double> curr: list){
				 hmap.put(curr.getKey(), curr.getValue());
			 }
			 for(String key: hmap.keySet()){
				 con.write(new Text("Movie ID "+key+": "),new DoubleWritable(hmap.get(key)));
			 }
		 }
	
	}

}
