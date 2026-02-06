
# Project 1: Big Data Management & Analytics – “Are You My Friend Analytics”

## Project Overview
This project implements **big data analytics on a Facebook-like dataset** using **Hadoop MapReduce**.  

We set up a Hadoop cluster on a local VM, loaded sample datasets into HDFS, and wrote Java MapReduce programs to answer eight analytics tasks (Tasks a–h).  

The goal is to practice **scalable data processing**, **HDFS file management**, and **MapReduce programming**, while documenting results and validating correctness.  

---

## SSH & Hadoop Environment Setup (Cheat Sheet)

### 1. SSH into VM
```bash
ssh -p 14226 cs4433@localhost
password: cs4433
````
As long as the docker for CS4433 is running you can always ssh into the local host.
locaohost link: http://localhost:14228/dfshealth.html#tab-overview


### 2. Check Java Hadoop processes

```bash
jps
```

Expected output:

```
NameNode
DataNode
ResourceManager
SecondaryNameNode
JobHistoryServer
Jps
```

### 3. Start Hadoop

```bash
/opt/start-hadoop.sh
```

### 4. Stop Hadoop

```bash
/opt/stop-hadoop.sh
```

### 5. HDFS Commands

#### Create directories

```bash
hdfs dfs -mkdir -p /user/cs4433/input
```

#### Upload CSV files to HDFS

```bash
hdfs dfs -put pages.csv /user/cs4433/input/
hdfs dfs -put friends.csv /user/cs4433/input/
hdfs dfs -put access_logs.csv /user/cs4433/input/
```

#### List files

```bash
hdfs dfs -ls /user/cs4433/input
```

#### Check block distribution

```bash
hdfs fsck /user/cs4433/input
```

#### Remove output directories (before running jobs)

```bash
hdfs dfs -rm -r /user/cs4433/output_taskA
hdfs dfs -rm -r /user/cs4433/output_taskB
# Repeat for other tasks
```

---

## Running Hadoop MapReduce Tasks

### General Command Template

```bash
hadoop jar CS4433_Project1.jar <TaskClass> <input1> <input2 ...> <output>
```

* `<TaskClass>` → Java class implementing the Task (e.g., `TaskE`)
* `<inputX>` → HDFS path(s) to input CSV(s)
* `<output>` → HDFS path to store output

---

### Example: Task E – People’s Favorites

```bash
# Remove output directory first
hdfs dfs -rm -r /user/cs4433/output_taskE

# Run TaskE
hadoop jar CS4433_Project1.jar TaskE \
    /user/cs4433/input/pages.csv \
    /user/cs4433/input/access_logs.csv \
    /user/cs4433/output_taskE
```

* **Input:** `pages.csv`, `access_logs.csv`
* **Output:** Name of each person, total page accesses, and distinct pages accessed

Check output:

```bash
hdfs dfs -cat /user/cs4433/output_taskE/part-r-00000 | head -n 20
```

---

### Running Other Tasks

| Task   | Description                           | Command Example                                                                                                                                                 |
| ------ | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Task A | Users of the same nationality         | `hadoop jar CS4433_Project1.jar TaskA /user/cs4433/input/pages.csv /user/cs4433/output_taskA`                                                                   |
| Task B | Top 10 popular Facebook pages         | `hadoop jar CS4433_Project1.jar TaskB /user/cs4433/input/access_logs.csv /user/cs4433/input/pages.csv /user/cs4433/output_taskB`                                |
| Task C | Number of citizens per country        | `hadoop jar CS4433_Project1.jar TaskC /user/cs4433/input/pages.csv /user/cs4433/output_taskC`                                                                   |
| Task D | Connectedness factor                  | `hadoop jar CS4433_Project1.jar TaskD /user/cs4433/input/friends.csv /user/cs4433/input/pages.csv /user/cs4433/output_taskD`                                    |
| Task E | Favorites (total + distinct accesses) | `hadoop jar CS4433_Project1.jar TaskE /user/cs4433/input/pages.csv /user/cs4433/input/access_logs.csv /user/cs4433/output_taskE`                                |
| Task F | Friends never accessed                | `hadoop jar CS4433_Project1.jar TaskF /user/cs4433/input/pages.csv /user/cs4433/input/friends.csv /user/cs4433/input/access_logs.csv /user/cs4433/output_taskF` |
| Task G | Disconnected people                   | `hadoop jar CS4433_Project1.jar TaskG /user/cs4433/input/pages.csv /user/cs4433/input/access_logs.csv /user/cs4433/output_taskG`                                |
| Task H | Popular people above average          | `hadoop jar CS4433_Project1.jar TaskH /user/cs4433/input/pages.csv /user/cs4433/input/friends.csv /user/cs4433/output_taskH`                                    |

> **Note:** Remove the corresponding output directory before running each job.

---

## Validating Output

1. Preview results using:

```bash
hdfs dfs -cat /user/cs4433/output_taskE/part-r-00000 | head -n 20
```

2. Compare a few records manually with the CSV input to ensure correctness.
3. Take **screenshots** for the project report.

---

## Project Submission

1. **Code:** Zip the IntelliJ project folder (exclude large datasets).
2. **REPORT:** Include

    * Dataset preparation and HDFS screenshots
    * Screenshots of outputs
    * Resource usage statement (who did what)
    * Contribution statement
    * Notes on scalable MapReduce solutions and combiner usage
3. **Peer Feedback:** Submit via Google Form.

---

## Notes on Optimizations

* Use combiners wherever possible to reduce I/O (not all tasks can use them).
* Minimize multiple MapReduce jobs if a single job suffices.
* Some tasks can be implemented as **Map-only jobs** for better performance.

---

### References

* [Hadoop File System Commands](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html)
* Class examples: WordCount, MapReduce framework

