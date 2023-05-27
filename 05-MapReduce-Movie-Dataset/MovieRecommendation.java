import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRecommendationJob {

  public static class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    private DoubleWritable rating = new DoubleWritable();
    private Text movieId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Split the input line by comma
      String[] fields = value.toString().split(",");
      
      // Extract movie ID and average rating
      movieId.set(fields[0]);
      rating.set(Double.parseDouble(fields[1]));
      
      // Emit (rating, movieId) key-value pair
      context.write(rating, movieId);
    }
  }

  public static class RecommendationReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    private int counter = 0;

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Iterate over movies with the same rating in descending order
      for (Text value : values) {
        // Emit (movieId, rating) key-value pair
        context.write(value, key);
        
        // Increment counter
        counter++;
        
        // Limit the number of recommendations to, for example, top 10 movies
        if (counter >= 10) {
          break;
        }
      }
    }
  }
}
