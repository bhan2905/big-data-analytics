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

public class bai2 {

    // Mapper for movie dataset
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 3);
            if (parts.length >= 3) {
                String movieId = parts[0].trim();
                String genreList = parts[2].trim();

                String[] genres = genreList.split("\\|"); //Split genres
                for (String g : genres) {
                    context.write(new Text(movieId), new Text ("G:" + g)); // Emit movieId as key and genre as value with prefix G:
                }
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
    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Double> genreSum = new TreeMap<>(); // Map to store total ratings
        private Map<String, Integer> genreCount = new TreeMap<>(); // Map to store rating counts

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            List<String> currentGenres = new ArrayList<>();
            List<Double> currentRatings = new ArrayList<>();

            for (Text val : values) {
                String data = val.toString();
                if (data.startsWith("G:")) {
                    currentGenres.add(data.substring(2)); // Extract genre
                } else if (data.startsWith("R:")) {
                    currentRatings.add(Double.parseDouble(data.substring(2))); // Convert rating to double
                }
            }

            if (!currentGenres.isEmpty() && !currentRatings.isEmpty()) {
                for (String genre : currentGenres) {
                    for (Double rating : currentRatings) {
                        genreSum.put(genre, genreSum.getOrDefault(genre, 0.0) + rating); // Update total rating
                        genreCount.put(genre, genreCount.getOrDefault(genre, 0) + 1); // Update count
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry : genreSum.entrySet()) {
                String genre = entry.getKey();
                double totalRating = entry.getValue();
                int count = genreCount.get(genre);
                
                double average = totalRating / count; // Calculate average rating

                String resultValue = String.format("Avg: %.2f, Count: %d", average, count);
                
                context.write(new Text(genre), new Text(resultValue)); // Format output
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Analysis");
        job.setJarByClass(bai2.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class); // Input path for movie dataset
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class); // Input path for ratings dataset

        job.setReducerClass(GenreReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}