import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Set<String> uniqueWords = new HashSet<>();
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String word = key.toString();
            uniqueWords.add(word);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            result.set(uniqueWords.size());
            context.write(new Text("Unique Listeners Count: "), result);
        }
}