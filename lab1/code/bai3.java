import java.io.IOException;
import java.util.*;

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

public class bai3 {

    // Mapper for rating dataset
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[0].trim()), new Text("R:" + parts[1].trim() + ":" + parts[2].trim())); // Emit userId as key, and R:movieId:rating as value
            }
        }
    }

    // Mapper for user dataset
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("G:" + parts[1].trim())); // Emit userId as key, and gender as value 
            }
        }
    }

    // Reducer to join ratings with users to get gender and rating
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String gender = "";
            List<String> ratings = new ArrayList<>();
            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("G:")) gender = str.substring(2); // Extract gender
                else if (str.startsWith("R:")) ratings.add(str.substring(2)); // Extract movieId and rating
            }
            if (!gender.isEmpty()) {
                for (String r : ratings) {
                    String[] rParts = r.split(":");
                    context.write(new Text(rParts[0]), new Text(gender + ":" + rParts[1]));
                }
            }
        }
    }

    // Mapper for joining result of user and rating datasets
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t"); 
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("D:" + parts[1].trim())); // Emit movieId as key, and D:gender:rating as value
            }
        }
    }

    // Mapper for movie dataset
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 3);
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("T:" + parts[1].trim())); // Emit movieId as key, and T:title as value
            }
        }
    }

    // Reducer to output final result 
    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {

        private TreeMap<String, String> SortMap = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) {
            String title = "";
            List<String> dataList = new ArrayList<>();
            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("T:")) title = str.substring(2); // Extract title
                else if (str.startsWith("D:")) dataList.add(str.substring(2)); // Extract gender and rating
            }

            if (!title.isEmpty() && !dataList.isEmpty()) {
                double mSum = 0, fSum = 0;
                int mCount = 0, fCount = 0;
                for (String d : dataList) {
                    String[] p = d.split(":");
                    double score = Double.parseDouble(p[1]);
                    if (p[0].equalsIgnoreCase("M")) { 
                        mSum += score; mCount++; // Calculate sum and count for male
                    } else {
                        fSum += score; fCount++; // Calculate sum and count for female
                    }
                }
                String res = String.format("Male: %.2f, Female: %.2f", 
                             (mCount > 0 ? mSum/mCount : 0.0), (fCount > 0 ? fSum/fCount : 0.0));
                SortMap.put(title, res);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, String> e : SortMap.entrySet()) {
                context.write(new Text(e.getKey()), new Text(e.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path tempDir = new Path("temp_job");

        Job job1 = Job.getInstance(conf, "Job 1: Ratings-Users Join");
        job1.setJarByClass(bai3.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapper.class);
        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, tempDir);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "Job 2: Final Result Gender");
            job2.setJarByClass(bai3.class);
            MultipleInputs.addInputPath(job2, tempDir, TextInputFormat.class, JoinMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);
            job2.setReducerClass(GenderReducer.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}