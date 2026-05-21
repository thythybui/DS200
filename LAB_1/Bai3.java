import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Bai3 {


    public static class RatingMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable userIdKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Schema: UserID, MovieID, Rating, Timestamp
            String[] parts = line.split(",");
            if (parts.length < 3) return;

            try {
                int userId  = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                float rating = Float.parseFloat(parts[2].trim());

                userIdKey.set(userId);
                outValue.set("R|" + movieId + "|" + rating);
                context.write(userIdKey, outValue);
            } catch (NumberFormatException ignored) {}
        }
    }

    
    public static class UserMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable userIdKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Schema: UserID, Gender, Age, Occupation, Zip
            String[] parts = line.split(",");
            if (parts.length < 2) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                String gender = parts[1].trim(); // "M" hoặc "F"

                userIdKey.set(userId);
                outValue.set("U|" + gender);
                context.write(userIdKey, outValue);
            } catch (NumberFormatException ignored) {}
        }
    }

    
    public static class UserRatingJoinReducer extends Reducer<IntWritable, Text, Text, FloatWritable> {

        private final Text outKey = new Text();
        private final FloatWritable outVal = new FloatWritable();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String gender = null;
            List<String> ratingRecords = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("U|")) {
                    gender = v.substring(2); // "M" hoặc "F"
                } else if (v.startsWith("R|")) {
                    ratingRecords.add(v.substring(2)); // "MovieID|Rating"
                }
            }

            if (gender == null || ratingRecords.isEmpty()) return;

            for (String record : ratingRecords) {
                String[] parts = record.split("\\|");
                if (parts.length < 2) continue;
                try {
                    String movieId = parts[0].trim();
                    float rating = Float.parseFloat(parts[1].trim());

                    // Emit: "MovieID|Gender" -> rating
                    outKey.set(movieId + "|" + gender);
                    outVal.set(rating);
                    context.write(outKey, outVal);
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    
    public static class RatingGenderMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable movieIdKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Format: "MovieID|Gender\trating"
            String[] tabParts = line.split("\t");
            if (tabParts.length < 2) return;

            String[] keyParts = tabParts[0].split("\\|");
            if (keyParts.length < 2) return;

            try {
                int movieId = Integer.parseInt(keyParts[0].trim());
                String gender = keyParts[1].trim();
                float rating = Float.parseFloat(tabParts[1].trim());

                movieIdKey.set(movieId);
                outValue.set("RG|" + gender + "|" + rating);
                context.write(movieIdKey, outValue);
            } catch (NumberFormatException ignored) {}
        }
    }

   
    public static class MovieMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable movieIdKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Schema: MovieID, Title, Genres
            String[] parts = line.split(",", 3);
            if (parts.length < 2) return;

            try {
                int movieId = Integer.parseInt(parts[0].trim());
                String title = parts[1].trim();

                movieIdKey.set(movieId);
                outValue.set("M|" + title);
                context.write(movieIdKey, outValue);
            } catch (NumberFormatException ignored) {}
        }
    }

    public static class GenderAggReducer extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String movieTitle = "";
            float maleSum = 0.0f;
            int maleCount = 0;
            float femaleSum = 0.0f;
            int femaleCount = 0;

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("M|")) {
                    movieTitle = v.substring(2);

                } else if (v.startsWith("RG|")) {
                    // Format: "RG|Gender|Rating"
                    String[] parts = v.substring(3).split("\\|");
                    if (parts.length < 2) continue;
                    try {
                        String gender = parts[0].trim();
                        float rating = Float.parseFloat(parts[1].trim());

                        if ("M".equals(gender)) {
                            maleSum += rating;
                            maleCount++;
                        } else if ("F".equals(gender)) {
                            femaleSum += rating;
                            femaleCount++;
                        }
                    } catch (NumberFormatException ignored) {}
                }
            }

            if (movieTitle.isEmpty()) return;

            String maleAvg   = maleCount   > 0 ? String.format("%.2f", maleSum   / maleCount)   : "N/A";
            String femaleAvg = femaleCount  > 0 ? String.format("%.2f", femaleSum / femaleCount)  : "N/A";

            context.write(
                    new Text(movieTitle),
                    new Text("Male_Avg=" + maleAvg + ", Female_Avg=" + femaleAvg)
            );
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.err.println("Usage: Bai3 <ratings_input> <users_input> <movies_input> <intermediate_output> <final_output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // --- Job 1: Join ratings + users theo UserID ---
        Job job1 = Job.getInstance(conf, "Bai3 - Job1: Join Ratings + Users");
        job1.setJarByClass(Bai3.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapper.class);

        job1.setReducerClass(UserRatingJoinReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[3]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // --- Job 2: Join với movies, aggregate Male/Female avg ---
        Job job2 = Job.getInstance(conf, "Bai3 - Job2: Gender Avg per Movie");
        job2.setJarByClass(Bai3.class);

        // Đọc output Job 1 + movies.txt với 2 mapper khác nhau
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, RatingGenderMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);

        job2.setReducerClass(GenderAggReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
