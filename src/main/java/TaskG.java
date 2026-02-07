import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskG {

    private static final String GLOBAL_KEY = "__GLOBAL_MAX__";
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // =========================
    // JOB 1: AccessLogs -> last access per person + global max
    // =========================
    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.startsWith("AccessID") || line.startsWith("AccessId")) return;

            // access_logs.csv: AccessID,ByWho,WhatPage,TypeOfAccess,AccessTime
            String[] tokens = line.split(",", -1);
            if (tokens.length < 5) return;

            String byWho = tokens[1].trim();
            String accessTime = tokens[4].trim();
            if (byWho.isEmpty() || accessTime.isEmpty()) return;

            // person -> timestamp
            context.write(new Text(byWho), new Text(accessTime));

            // global max -> timestamp
            context.write(new Text(GLOBAL_KEY), new Text(accessTime));
        }
    }

    public static class LastAccessReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LocalDateTime max = null;

            for (Text v : values) {
                String ts = v.toString().trim();
                if (ts.isEmpty()) continue;
                try {
                    LocalDateTime t = LocalDateTime.parse(ts, TS_FMT);
                    if (max == null || t.isAfter(max)) max = t;
                } catch (Exception ignored) {
                    // skip bad timestamps
                }
            }

            if (max != null) {
                context.write(key, new Text(max.format(TS_FMT)));
            }
        }
    }

    // =========================
    // JOB 2: Join pages.csv with last-access output,
    // filter by (refTime - lastAccess) >= 14 days OR no access record
    // =========================

    // pages.csv -> PersonID -> NAME:<name>
    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("PersonID")) return;

            // pages.csv: PersonID,Name,...
            String[] tokens = line.split(",", -1);
            if (tokens.length < 2) return;

            String personId = tokens[0].trim();
            String name = tokens[1].trim();
            if (personId.isEmpty() || name.isEmpty()) return;

            context.write(new Text(personId), new Text("NAME:" + name));
        }
    }

    // last-access output -> PersonID -> LAST:<timestamp>
    public static class LastAccessMapper2 extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Format from Job1: key \t timestamp
            String[] parts = line.split("\t");
            if (parts.length < 2) return;

            String personId = parts[0].trim();
            String ts = parts[1].trim();
            if (personId.isEmpty() || ts.isEmpty()) return;

            // Skip global key in join stage
            if (GLOBAL_KEY.equals(personId)) return;

            context.write(new Text(personId), new Text("LAST:" + ts));
        }
    }

    public static class TaskGReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String refTimeStr = conf.get("taskg.refTime", "");
            if (refTimeStr.isEmpty()) return;

            LocalDateTime refTime;
            try {
                refTime = LocalDateTime.parse(refTimeStr, TS_FMT);
            } catch (Exception e) {
                return;
            }

            String name = "";
            LocalDateTime last = null;

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("NAME:")) {
                    name = s.substring("NAME:".length());
                } else if (s.startsWith("LAST:")) {
                    String ts = s.substring("LAST:".length()).trim();
                    try {
                        LocalDateTime t = LocalDateTime.parse(ts, TS_FMT);
                        if (last == null || t.isAfter(last)) last = t;
                    } catch (Exception ignored) {}
                }
            }

            if (name.isEmpty()) return; // only output people that exist in pages.csv

            // If no access ever -> disconnected
            if (last == null) {
                context.write(key, new Text(name));
                return;
            }

            long days = Duration.between(last, refTime).toDays();
            if (days >= 14) {
                context.write(key, new Text(name));
            }
        }
    }

    // =========================
    // Helper: read global max time from Job1 output in HDFS
    // =========================
    private static String readGlobalMax(Configuration conf, Path job1Out) throws IOException {
        FileSystem fs = job1Out.getFileSystem(conf);

        // Typically: part-r-00000 (since we set 1 reducer)
        Path part = new Path(job1Out, "part-r-00000");
        if (!fs.exists(part)) {
            // fallback: scan all part files
            FileStatus[] statuses = fs.listStatus(job1Out, p -> p.getName().startsWith("part-"));
            if (statuses == null || statuses.length == 0) return "";
            part = statuses[0].getPath();
        }

        try (FSDataInputStream in = fs.open(part);
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 2 && GLOBAL_KEY.equals(parts[0].trim())) {
                    return parts[1].trim();
                }
            }
        }
        return "";
    }

    // =========================
    // Driver
    // args: <pages.csv> <access_logs.csv> <output>
    // =========================
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TaskG <pages.csv> <access_logs.csv> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Path pagesPath = new Path(args[0]);
        Path accessPath = new Path(args[1]);
        Path finalOut = new Path(args[2]);

        // temp path for Job1 output
        Path tmpOut = new Path("/tmp/taskg_lastaccess_" + System.currentTimeMillis());

        // -------------------------
        // Job 1
        // -------------------------
        Job job1 = Job.getInstance(conf, "TaskG-Job1-LastAccessAndGlobalMax");
        job1.setJarByClass(TaskG.class);

        job1.setMapperClass(AccessLogMapper.class);
        job1.setReducerClass(LastAccessReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setNumReduceTasks(1);

        TextInputFormat.addInputPath(job1, accessPath);
        FileOutputFormat.setOutputPath(job1, tmpOut);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Read global max timestamp from job1 output and pass to job2
        String refTime = readGlobalMax(conf, tmpOut);
        if (refTime.isEmpty()) {
            System.err.println("Could not determine global max AccessTime from access logs.");
            System.exit(1);
        }

        // -------------------------
        // Job 2
        // -------------------------
        Configuration conf2 = new Configuration(conf);
        conf2.set("taskg.refTime", refTime);

        Job job2 = Job.getInstance(conf2, "TaskG-Job2-Disconnected(>=14days)");
        job2.setJarByClass(TaskG.class);

        // Multiple inputs: pages + job1 output (per-person last access)
        MultipleInputs.addInputPath(job2, pagesPath, TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job2, tmpOut, TextInputFormat.class, LastAccessMapper2.class);

        job2.setReducerClass(TaskGReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job2, finalOut);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
