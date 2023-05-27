import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
	
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String user = line.split(",")[0];
            
            if(user.length()>0) {
                word.set(user);
                context.write(word, one);
            }
        }
    }