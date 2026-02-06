import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskE {

    // =========================
    // Mapper for pages.csv
    // Output: PersonID -> NAME:name
    // =========================
    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.startsWith("PersonID") || line.isEmpty()) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 2) return;

            String personId = tokens[0].trim();
            String name = tokens[1].trim();

            context.write(new Text(personId), new Text("NAME:" + name));
        }
    }

    // =========================
    // Mapper for access_logs.csv
    // Output: ByWho -> ACCESS:PageID
    // =========================
    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.startsWith("AccessID") || line.isEmpty()) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 3) return;

            String byWho = tokens[1].trim();   // PersonID
            String whatPage = tokens[2].trim(); // Page accessed

            context.write(new Text(byWho), new Text("ACCESS:" + whatPage));
        }
    }

    // =========================
    // Reducer
    // =========================
    public static class TaskEReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String name = "";
            int totalAccess = 0;
            Set<String> distinctPages = new HashSet<>();

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("NAME:")) {
                    name = s.substring(5);
                } else if (s.startsWith("ACCESS:")) {
                    totalAccess++;
                    distinctPages.add(s.substring(7));
                }
            }

            if (!name.isEmpty()) {
                context.write(new Text(name),
                        new Text(totalAccess + "\t" + distinctPages.size()));
            }
        }
    }

    // =========================
    // Driver
    // =========================
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TaskE <pages.csv> <access_logs.csv> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task E - Favorites");

        job.setJarByClass(TaskE.class);

        // Multiple input files
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessMapper.class);

        // Reducer
        job.setReducerClass(TaskEReducer.class);

        // Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 1 reducer for simplicity
        job.setNumReduceTasks(1);

        // Output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
