import java.io.IOException;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1 {

    // Mapper for movie dataset
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 3); // Split into 3 parts with commas
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("M:" + parts[1].trim())); // Emit movieId as key and movie title as value with prefix M:
            }
        }
    }

    // Mapper for ratings dataset
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[1].trim()), new Text("R:" + parts[2].trim())); // Emit movieId as key and rating as value with prefix R:
            }
        }
    }

    // Reducer
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private String maxMovie = "";
        private double maxRating = -1.0;

        private TreeMap<String, String> finalResults = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown";
            ArrayList<Double> ratingsList = new ArrayList<>();

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("M:")) {
                    movieTitle = strVal.substring(2); // Extract movie title by removing M: prefix
                } else if (strVal.startsWith("R:")) { // // Extract rating by removing R: prefix
                    try {
                        ratingsList.add(Double.parseDouble(strVal.substring(2))); // Convert rating to double
                    } catch (NumberFormatException e) {}
                }
            }

            if (!movieTitle.equals("Unknown") && !ratingsList.isEmpty()) {
                double sum = 0;
                for (double r : ratingsList) sum += r;
                int count = ratingsList.size();
                double avg = sum / count; // Calculate average rating

                String info = "Average rating: " + String.format("%.2f", avg) + " (Total ratings: " + count + ")"; 
                finalResults.put(movieTitle, info); // Format output

                if (count >= 5 && avg > maxRating) { // Update for the highest average rated movie with at least 5 ratings
                    maxRating = avg;
                    maxMovie = movieTitle;
                }
            }
        }

        // Add cleanup to output final results
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<String, String> entry : finalResults.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }

            if (!maxMovie.equals("")) {
                context.write(new Text("\n" +maxMovie),
                    new Text("is the highest rated movie with an average rating of " + String.format("%.2f", maxRating) + " among movies with at least 5 ratings"));
            } else {
                context.write(new Text("\nNotice:"), 
                new Text("No movie found with at least 5 ratings.")); // Output notice if no movie meets the criteria
    }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Analysis");
        job.setJarByClass(bai1.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class); // Input path for movie dataset
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class); // Input path for ratings dataset

        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}