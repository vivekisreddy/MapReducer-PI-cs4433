import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskA {

    public static class NationalityMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private static final String TARGET_NATIONALITY = "Russia";
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Skip header line
            if (isHeader) {
                isHeader = false;
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",", -1);

            // Defensive check
            if (fields.length < 5) {
                return;
            }

            String name = fields[1].trim();
            String nationality = fields[2].trim();
            String hobby = fields[4].trim();

            if (nationality.equals(TARGET_NATIONALITY)) {
                context.write(new Text(name), new Text(hobby));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: TaskA <program> <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task A - Filter by Nationality");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(NationalityMapper.class);

        // Map-only job
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // IMPORTANT FIX
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

