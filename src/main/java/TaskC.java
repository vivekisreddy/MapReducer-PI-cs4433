import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskC {

    /**
     * Mapper
     * Input: pages.csv
     * Output: <Nationality, 1>
     */
    public static class CountryMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Skip header
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",", -1);

            // Defensive check
            if (fields.length < 5) return;

            String nationality = fields[2].trim();
            context.write(new Text(nationality), ONE);
        }
    }

    /**
     * Reducer
     * Input: <Nationality, [1,1,1,...]>
     * Output: <Nationality, total_count>
     */
    public static class CountryReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: TaskC <pages_csv> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task C - Count Facebook Pages per Country");

        job.setJarByClass(TaskC.class);
        job.setMapperClass(CountryMapper.class);
        job.setReducerClass(CountryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
