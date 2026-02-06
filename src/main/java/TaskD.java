import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskD {

    /* =========================
       Mapper for pages.csv
       =========================
       Input: PersonID,Name,...
       Output: (PersonID, "NAME:Name")
    */
    public static class PageMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("PersonID")) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 2) return;

            String personId = tokens[0].trim();
            String name = tokens[1].trim();

            context.write(new Text(personId), new Text("NAME:" + name));
        }
    }

    /* =========================
       Mapper for friends.csv
       =========================
       Input: p1,p2
       Output: (p2, "FRIEND:1")
    */
    public static class FriendMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("FriendRel")) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 3) return;

            // MyFriend is column index 2
            String p2 = tokens[2].trim();

            context.write(new Text(p2), new Text("FRIEND:1"));
        }
    }


    /* =========================
       Reducer
       =========================
       Output: Name \t count
    */
    public static class TaskDReducer
            extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String name = "";
            int count = 0;

            for (Text v : values) {
                String val = v.toString();

                if (val.startsWith("NAME:")) {
                    name = val.substring(5);
                } else if (val.startsWith("FRIEND:")) {
                    count += 1;
                }
            }

            // Even if count == 0, this reducer still runs
            if (!name.isEmpty()) {
                context.write(new Text(name), new IntWritable(count));
            }
        }
    }

    /* =========================
       Driver
       ========================= */
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: TaskD <pages.csv> <friends.csv> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task D");

        job.setJarByClass(TaskD.class);

        // ===== MULTIPLE INPUTS =====
        MultipleInputs.addInputPath(
                job,
                new Path(args[0]),
                TextInputFormat.class,
                PageMapper.class);

        MultipleInputs.addInputPath(
                job,
                new Path(args[1]),
                TextInputFormat.class,
                FriendMapper.class);

        // ===== REDUCER =====
        job.setReducerClass(TaskDReducer.class);

        // ===== MAP OUTPUT TYPES (CRITICAL FIX) =====
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // ===== FINAL OUTPUT TYPES =====
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
