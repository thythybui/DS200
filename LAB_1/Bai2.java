import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;

public class Bai2 {

    public static class RatingMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable movieIdKey = new IntWritable();
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
                int movieId = Integer.parseInt(parts[1].trim());
                float rating = Float.parseFloat(parts[2].trim());

                movieIdKey.set(movieId);
                outValue.set("R|" + rating);
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
            if (parts.length < 3) return;

            try {
                int movieId = Integer.parseInt(parts[0].trim());
                String genres = parts[2].trim(); // "Action|Sci-Fi|..."

                movieIdKey.set(movieId);
                outValue.set("M|" + genres);
                context.write(movieIdKey, outValue);
            } catch (NumberFormatException ignored) {}
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, Text, FloatWritable> {

        private final Text genreKey = new Text();
        private final FloatWritable ratingValue = new FloatWritable();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] genres = null;
            java.util.List<Float> ratings = new java.util.ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M|")) {
                    genres = v.substring(2).split("\\|");
                } else if (v.startsWith("R|")) {
                    try {
                        ratings.add(Float.parseFloat(v.substring(2)));
                    } catch (NumberFormatException ignored) {}
                }
            }

            if (genres == null || ratings.isEmpty()) return;

            // Emit mỗi genre với từng rating
            for (String genre : genres) {
                genreKey.set(genre.trim());
                for (float r : ratings) {
                    ratingValue.set(r);
                    context.write(genreKey, ratingValue);
                }
            }
        }
    }


    public static class GenreAggMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final Text genreKey = new Text();
        private final FloatWritable ratingVal = new FloatWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Đọc output Job 1: "Genre\trating"
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length < 2) return;

            try {
                genreKey.set(parts[0].trim());
                ratingVal.set(Float.parseFloat(parts[1].trim()));
                context.write(genreKey, ratingVal);
            } catch (NumberFormatException ignored) {}
        }
    }

    public static class GenreAggReducer extends Reducer<Text, FloatWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0.0f;
            int count = 0;

            for (FloatWritable r : values) {
                sum += r.get();
                count++;
            }

            float avg = sum / count;
            context.write(
                    key,
                    new Text(String.format("%.2f", avg) + " (TotalRatings: " + count + ")")
            );
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: Bai2 <ratings_input> <movies_input> <intermediate_output> <final_output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // --- Job 1: Join ---
        Job job1 = Job.getInstance(conf, "Bai2 - Job1: Join Ratings + Movies");
        job1.setJarByClass(Bai2.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        job1.setReducerClass(JoinReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // --- Job 2: Aggregate per genre ---
        Job job2 = Job.getInstance(conf, "Bai2 - Job2: Avg Rating per Genre");
        job2.setJarByClass(Bai2.class);

        job2.setMapperClass(GenreAggMapper.class);
        job2.setReducerClass(GenreAggReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(FloatWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
