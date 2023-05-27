import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageRatingJob {

  public static class RatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    
    private Text movieId = new Text();
    private DoubleWritable rating = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Split the input line by comma (assuming CSV format)
      String[] fields = value.toString().split(",");
      
      // Extract movie ID and rating
      movieId.set(fields[0]);
      rating.set(Double.parseDouble(fields[2]));
      
      // Emit (movieId, rating) key-value pair
      context.write(movieId, rating);
    }
  }

  public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable averageRating = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double sum = 0.0;
      int count = 0;
      
      // Calculate the sum and count of ratings for each movie
      for (DoubleWritable value : values) {
        sum += value.get();
        count++;
      }
      
      // Calculate the average rating
      double avg = sum / count;
      
      averageRating.set(avg);
      
      // Emit (movieId, averageRating) key-value pair
      context.write(key, averageRating);
    }
  }
}
