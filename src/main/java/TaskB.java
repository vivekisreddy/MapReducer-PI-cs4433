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

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TaskB {

    // Mapper: emits <pageId, 1>
    public static class AccessLogMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable pageId = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("AccessID")) return; // skip header
            String[] fields = value.toString().split(",");
            int pid = Integer.parseInt(fields[2]);
            pageId.set(pid);
            context.write(pageId, one);
        }
    }

    // Reducer: sums accesses and joins with page info
    public static class AccessLogReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
        private Map<Integer, String[]> pages = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Load pages.csv from distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                File cachedFile = new File("./" + new File(cacheFiles[0].getPath()).getName());
                try (BufferedReader br = new BufferedReader(new FileReader(cachedFile))) {
                    br.readLine(); // skip header
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] fields = line.split(",");
                        int id = Integer.parseInt(fields[0]);
                        String name = fields[1];
                        String nationality = fields[2];
                        pages.put(id, new String[]{name, nationality});
                    }
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            String[] pageInfo = pages.get(key.get());
            if (pageInfo != null) {
                context.write(new Text(key.get() + "\t" + pageInfo[0] + "\t" + pageInfo[1]), new IntWritable(sum));
            }
        }
    }

    // Main
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TaskB <access_log> <pages_csv> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Task B - Count Accesses and Join Pages");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add pages.csv to distributed cache
        job.addCacheFile(new Path(args[1]).toUri());

        // Input path: only the access log
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

