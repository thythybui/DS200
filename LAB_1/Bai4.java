import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Bai4 {

    private static String getAgeGroup(int age) {
        if (age <= 18)       return "0-18";
        else if (age <= 35)  return "18-35";
        else if (age <= 50)  return "35-50";
        else                 return "50+";
    }

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
            if (parts.length < 3) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int age    = Integer.parseInt(parts[2].trim());

                String ageGroup = getAgeGroup(age);

                userIdKey.set(userId);
                outValue.set("U|" + ageGroup);
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

            String ageGroup = null;
            List<String> ratingRecords = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("U|")) {
                    ageGroup = v.substring(2); // "0-18", "18-35", "35-50", "50+"
                } else if (v.startsWith("R|")) {
                    ratingRecords.add(v.substring(2)); // "MovieID|Rating"
                }
            }

            if (ageGroup == null || ratingRecords.isEmpty()) return;

            for (String record : ratingRecords) {
                String[] parts = record.split("\\|");
                if (parts.length < 2) continue;
                try {
                    String movieId = parts[0].trim();
                    float rating   = Float.parseFloat(parts[1].trim());

                    outKey.set(movieId + "|" + ageGroup);
                    outVal.set(rating);
                    context.write(outKey, outVal);
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    
    public static class RatingAgeMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable movieIdKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Format: "MovieID|AgeGroup\trating"
            String[] tabParts = line.split("\t");
            if (tabParts.length < 2) return;

            String[] keyParts = tabParts[0].split("\\|", 2);
            if (keyParts.length < 2) return;

            try {
                int movieId = Integer.parseInt(keyParts[0].trim());
                String ageGroup = keyParts[1].trim();
                float rating = Float.parseFloat(tabParts[1].trim());

                movieIdKey.set(movieId);
                outValue.set("RA|" + ageGroup + "|" + rating);
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

   
    public static class AgeGroupAggReducer extends Reducer<IntWritable, Text, Text, Text> {

        // Thứ tự các nhóm tuổi khi hiển thị
        private static final String[] AGE_GROUP_ORDER = {"0-18", "18-35", "35-50", "50+"};

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String movieTitle = "";

            // Map: ageGroup -> (sum, count)
            java.util.Map<String, float[]> groupData = new java.util.HashMap<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("M|")) {
                    movieTitle = v.substring(2);

                } else if (v.startsWith("RA|")) {
                    // Format: "RA|AgeGroup|Rating"
                    String[] parts = v.substring(3).split("\\|", 2);
                    if (parts.length < 2) continue;
                    try {
                        String ageGroup = parts[0].trim();
                        float rating    = Float.parseFloat(parts[1].trim());

                        groupData.putIfAbsent(ageGroup, new float[]{0.0f, 0.0f});
                        groupData.get(ageGroup)[0] += rating; // sum
                        groupData.get(ageGroup)[1] += 1;      // count
                    } catch (NumberFormatException ignored) {}
                }
            }

            if (movieTitle.isEmpty()) return;

            // Format output
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < AGE_GROUP_ORDER.length; i++) {
                String group = AGE_GROUP_ORDER[i];
                if (i > 0) sb.append(", ");
                if (groupData.containsKey(group)) {
                    float[] sc = groupData.get(group);
                    float avg = sc[0] / sc[1];
                    sb.append(group).append(": ").append(String.format("%.2f", avg));
                } else {
                    sb.append(group).append(": N/A");
                }
            }
            sb.append("]");

            context.write(new Text(movieTitle), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.err.println("Usage: Bai4 <ratings_input> <users_input> <movies_input> <intermediate_output> <final_output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // --- Job 1: Join ratings + users theo UserID ---
        Job job1 = Job.getInstance(conf, "Bai4 - Job1: Join Ratings + Users by Age");
        job1.setJarByClass(Bai4.class);

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

        // --- Job 2: Join với movies, aggregate avg per age group ---
        Job job2 = Job.getInstance(conf, "Bai4 - Job2: Age Group Avg per Movie");
        job2.setJarByClass(Bai4.class);

        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, RatingAgeMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);

        job2.setReducerClass(AgeGroupAggReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
