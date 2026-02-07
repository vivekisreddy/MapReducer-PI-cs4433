import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskH {

    private static final String GLOBAL = "__GLOBAL__";

    // =========================
    // JOB 1: Count friends per person + global sum/count
    // =========================
    public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("FriendRel")) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 3) return;

            String personId = tokens[1].trim();
            if (personId.isEmpty()) return;

            // Count one friendship for this person
            context.write(new Text(personId), new IntWritable(1));

            // Track global totals
            context.write(new Text(GLOBAL), new IntWritable(1));
        }
    }

    public static class FriendCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();

            context.write(key, new IntWritable(sum));
        }
    }

    // =========================
    // JOB 2: Join pages.csv with friend counts and filter by average
    // =========================
    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("PersonID")) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 2) return;

            context.write(new Text(tokens[0].trim()),
                    new Text("NAME:" + tokens[1].trim()));
        }
    }

    public static class FriendCountMapper2 extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t");
            if (parts.length != 2) return;
            if (GLOBAL.equals(parts[0])) return;

            context.write(new Text(parts[0]), new Text("COUNT:" + parts[1]));
        }
    }

    public static class TaskHReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            double avg = Double.parseDouble(conf.get("taskh.avg"));

            String name = "";
            int count = 0;

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("NAME:")) {
                    name = s.substring(5);
                } else if (s.startsWith("COUNT:")) {
                    count = Integer.parseInt(s.substring(6));
                }
            }

            if (!name.isEmpty() && count > avg) {
                context.write(key, new Text(name));
            }
        }
    }

    // =========================
    // Helper: compute average
    // =========================
    private static double computeAverage(Configuration conf, Path job1Out) throws IOException {
        FileSystem fs = job1Out.getFileSystem(conf);
        Path part = new Path(job1Out, "part-r-00000");

        int totalFriends = 0;
        int totalPeople = 0;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = line.split("\t");
                if (p[0].equals(GLOBAL)) {
                    totalFriends = Integer.parseInt(p[1]);
                } else {
                    totalPeople++;
                }
            }
        }

        return (double) totalFriends / totalPeople;
    }

    // =========================
    // Driver
    // args: <pages.csv> <friends.csv> <output>
    // =========================
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TaskH <pages.csv> <friends.csv> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Path pages = new Path(args[0]);
        Path friends = new Path(args[1]);
        Path out = new Path(args[2]);
        Path tmp = new Path("/tmp/taskh_counts_" + System.currentTimeMillis());

        // ---------- Job 1 ----------
        Job job1 = Job.getInstance(conf, "TaskH-Job1-FriendCounts");
        job1.setJarByClass(TaskH.class);
        job1.setMapperClass(FriendsMapper.class);
        job1.setReducerClass(FriendCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);

        TextInputFormat.addInputPath(job1, friends);
        FileOutputFormat.setOutputPath(job1, tmp);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // Compute average
        double avg = computeAverage(conf, tmp);

        // ---------- Job 2 ----------
        Configuration conf2 = new Configuration(conf);
        conf2.set("taskh.avg", Double.toString(avg));

        Job job2 = Job.getInstance(conf2, "TaskH-Job2-AboveAverage");
        job2.setJarByClass(TaskH.class);

        MultipleInputs.addInputPath(job2, pages, TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job2, tmp, TextInputFormat.class, FriendCountMapper2.class);

        job2.setReducerClass(TaskHReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job2, out);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
