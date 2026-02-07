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

public class TaskF {

    // =========================
    // Mapper for pages.csv
    // pages.csv: PersonID,Name,Nationality,CountryCode,Hobby
    // Output: p1 -> NAME:<name>
    // =========================
    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("PersonID")) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 2) return;

            String personId = tokens[0].trim();
            String name = tokens[1].trim();
            if (personId.isEmpty() || name.isEmpty()) return;

            context.write(new Text(personId), new Text("NAME:" + name));
        }
    }

    // =========================
    // Mapper for friends.csv
    // friends.csv: FriendRel,PersonID,MyFriend,DateofFriendship,Desc
    // Output: p1 -> FRIEND:<p2>
    // =========================
    public static class FriendsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("FriendRel")) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 3) return;

            String p1 = tokens[1].trim();      // PersonID
            String p2 = tokens[2].trim();      // MyFriend
            if (p1.isEmpty() || p2.isEmpty()) return;

            context.write(new Text(p1), new Text("FRIEND:" + p2));
        }
    }

    // =========================
    // Mapper for access_logs.csv
    // access_logs.csv: AccessId,ByWho,WhatPage,TypeOfAccess,AccessTime
    // Output: p1 -> ACCESS:<p2>
    // =========================
    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("AccessID") || line.startsWith("AccessId")) return;

            String[] tokens = line.split(",", -1);
            if (tokens.length < 3) return;

            String byWho = tokens[1].trim();     // p1
            String whatPage = tokens[2].trim();  // p2
            if (byWho.isEmpty() || whatPage.isEmpty()) return;

            context.write(new Text(byWho), new Text("ACCESS:" + whatPage));
        }
    }

    // =========================
    // Reducer
    // For each p1:
    //   collect FRIEND set and ACCESS set
    //   if exists p2 in FRIENDS that is NOT in ACCESSED -> output p1 and name
    // =========================
    public static class TaskFReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String name = "";
            Set<String> friends = new HashSet<>();
            Set<String> accessed = new HashSet<>();

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("NAME:")) {
                    name = s.substring("NAME:".length());
                } else if (s.startsWith("FRIEND:")) {
                    friends.add(s.substring("FRIEND:".length()));
                } else if (s.startsWith("ACCESS:")) {
                    accessed.add(s.substring("ACCESS:".length()));
                }
            }

            // Task F condition:
            // p1 declared at least one friend p2, and there exists a friend p2 whose page was never accessed by p1.
            if (!friends.isEmpty()) {
                boolean foundUnvisitedFriend = false;
                for (String p2 : friends) {
                    if (!accessed.contains(p2)) {
                        foundUnvisitedFriend = true;
                        break;
                    }
                }

                if (foundUnvisitedFriend) {
                    // Output: PersonID and Name (if name missing, still output ID with blank name)
                    context.write(key, new Text(name));
                }
            }
        }
    }

    // =========================
    // Driver
    // args: <pages.csv> <friends.csv> <access_logs.csv> <output>
    // =========================
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TaskF <pages.csv> <friends.csv> <access_logs.csv> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task F - Friends Never Visited");

        job.setJarByClass(TaskF.class);

        // Multiple inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AccessMapper.class);

        // Reducer
        job.setReducerClass(TaskFReducer.class);

        // Map output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // One reducer for simple, single output file
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
