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

public class bai4 {

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
            if (parts.length >= 3) {
                try {
                    int age = Integer.parseInt(parts[2].trim());
                    String ageGroup = "";
                    if (age <= 18) ageGroup = "0-18";  // Define age groups
                    else if (age <= 35) ageGroup = "18-35";
                    else if (age <= 50) ageGroup = "35-50";
                    else ageGroup = "50+";
                    
                    context.write(new Text(parts[0].trim()), new Text("A:" + ageGroup)); // Emit userId as key, and A:ageGroup as value
                } catch (NumberFormatException e) {}
            }
        }
    }

    // Reducer to join ratings with users to get age group and rating
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ageGroup = "";
            List<String> ratings = new ArrayList<>();
            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("A:")) ageGroup = str.substring(2); // Extract age group
                else if (str.startsWith("R:")) ratings.add(str.substring(2)); // Extract movieId and rating
            }
            if (!ageGroup.isEmpty()) {
                for (String r : ratings) {
                    String[] rParts = r.split(":");
                    context.write(new Text(rParts[0]), new Text(ageGroup + ":" + rParts[1]));
                }
            }
        }
    }

    // Mapper for joining result of user and rating datasets
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t"); 
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("D:" + parts[1].trim())); // Emit movieId as key, and D:ageGroup:rating as value
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
    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {
        private TreeMap<String, String> SortMap = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) {
            String title = "";
            List<String> dataList = new ArrayList<>();
            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("T:")) title = str.substring(2); // Extract title
                else if (str.startsWith("D:")) dataList.add(str.substring(2)); // Extract ageGroup and rating
            }

            if (!title.isEmpty() && !dataList.isEmpty()) {
                double s18 = 0, s35 = 0, s50 = 0, sPlus = 0;
                int c18 = 0, c35 = 0, c50 = 0, cPlus = 0;

                for (String d : dataList) {
                    String[] p = d.split(":");
                    String group = p[0]; // Age group
                    double score = Double.parseDouble(p[1]); // Rating score

                    if (group.equals("0-18")) { s18 += score; c18++; } // Calculate sum and count for each age group
                    else if (group.equals("18-35")) { s35 += score; c35++; }
                    else if (group.equals("35-50")) { s50 += score; c50++; }
                    else { sPlus += score; cPlus++; }
                }

                // Calculate average rating for each group and "NA" if no ratings in that group
                String avg18 = (c18 > 0) ? String.format("%.2f", s18/c18) : "NA";
                String avg35 = (c35 > 0) ? String.format("%.2f", s35/c35) : "NA";
                String avg50 = (c50 > 0) ? String.format("%.2f", s50/c50) : "NA";
                String avgPlus = (cPlus > 0) ? String.format("%.2f", sPlus/cPlus) : "NA";

                // Format output
                String finalResult = String.format("0-18: %s   18-35: %s   35-50: %s   50+: %s", 
                                     avg18, avg35, avg50, avgPlus);
                
                SortMap.put(title, finalResult);
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
        Path tempDir = new Path("temp_job_age");

        Job job1 = Job.getInstance(conf, "Job 1: Ratings-Users Join");
        job1.setJarByClass(bai4.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapper.class);
        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, tempDir);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "Job 2: Final Result Age Group");
            job2.setJarByClass(bai4.class);
            MultipleInputs.addInputPath(job2, tempDir, TextInputFormat.class, JoinMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);
            job2.setReducerClass(AgeReducer.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}