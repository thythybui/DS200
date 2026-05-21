import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;

public class Bai1 {

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
            } catch (NumberFormatException e) {
                // Bỏ qua dòng lỗi
            }
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
            String[] parts = line.split(",", 3); // tối đa 3 phần để title không bị split
            if (parts.length < 2) return;

            try {
                int movieId = Integer.parseInt(parts[0].trim());
                String title = parts[1].trim();

                movieIdKey.set(movieId);
                outValue.set("M|" + title);

                context.write(movieIdKey, outValue);
            } catch (NumberFormatException e) {
                // Bỏ qua dòng lỗi
            }
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, Text, Text> {

        // Biến lớp (class-level) để track phim có điểm cao nhất
        private static String maxMovie = "";
        private static float maxRating = 0.0f;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String movieTitle = "";
            float sum = 0.0f;
            int count = 0;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M|")) {
                    movieTitle = v.substring(2);
                } else if (v.startsWith("R|")) {
                    try {
                        float rating = Float.parseFloat(v.substring(2));
                        sum += rating;
                        count++;
                    } catch (NumberFormatException ignored) {}
                }
            }

            if (count > 0 && !movieTitle.isEmpty()) {
                float avg = sum / count;

                System.out.println("KẾT QUẢ:");
                System.out.println("Movie: " + movieTitle);
                System.out.println("Average: " + avg);
                System.out.println("--------------------------");

                context.write(
                        new Text(movieTitle),
                        new Text("AverageRating: " + String.format("%.2f", avg))
                );
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(
                        new Text("BEST_MOVIE"),
                        new Text(maxMovie + " is the highest rated movie with an average rating of "
                                + String.format("%.2f", maxRating)
                                + " among movies with at least 5 ratings.")
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Bai1 <ratings_input_dir> <movies_input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai1 - Avg Rating per Movie");

        job.setJarByClass(Bai1.class);

        // MultipleInputs: mỗi file dùng mapper riêng
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
