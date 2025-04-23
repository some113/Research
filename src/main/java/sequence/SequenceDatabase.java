package sequence;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.*;

import spmf.AlgoTKS;
import spmf.PatternTKS;
import Config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import static java.lang.System.exit;

public class SequenceDatabase {
    static TreeSet<PatternTKS> eventSet = new TreeSet<PatternTKS>((p1, p2) -> {
        if (p1.getPrefix().equals(p2.getPrefix())) {
            return 0;
        } else if (p1.support == p2.support) {
            String[] parts1 = p1.getPrefix().split("-1");
            String[] parts2 = p2.getPrefix().split("-1");
            return parts1.length - parts2.length;
        }
        return p1.support - p2.support;
    });
    private static void sequenceMining() {
        String output = System.getProperty("user.dir") + "/OutputData/events.txt";
        File sequenceFolder = new File(System.getProperty("user.dir") + "/OutputData/SequencesToMine/");
        File eventFolder = new File(System.getProperty("user.dir") + "/OutputData/Events/");
        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new LinkedList<>();

        if (!eventFolder.exists()) {
            eventFolder.mkdir();
        }
        BufferedWriter writer = null;

        // k is huge to get more pattern but still only get those qualified afterward
        int k = 15;

        for (File seqFol : sequenceFolder.listFiles()) {
//            System.out.println("Mining sequences from: " + seqFol.getAbsolutePath());

            writer = null;
            File hostEventFolder = new File(eventFolder.getAbsolutePath() + "/" + seqFol.getName());
            hostEventFolder.mkdir();

            for (File seqFile : seqFol.listFiles(file -> !file.getName().equals("all.txt"))) {
                Future<?> future = executor.submit(new SequenceMiningProcessor(seqFile));
                futures.add(future);
            }

            for (Future<?> future : futures) {
                try {
                    future.get(10, TimeUnit.MINUTES);
                } catch (ExecutionException e) {
                    System.out.println("Error when mining sequence: " + e.getMessage());
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    System.out.println("Timeout when mining sequence: " + e.getMessage());
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    System.out.println("Interrupted when mining sequence: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            Iterator<Future<?>> iter = futures.iterator();
            while (iter.hasNext()) {
                Future<?> future = iter.next();
                if (!future.isDone() && !future.isCancelled()) {
                    System.out.println("Failed at task: " + future);
                }
            }

//                writer.flush();
//                writer.close();
        }

    }
    public static void generateEventSequence() {
        //Read data
        String directoryPath = System.getProperty("user.dir") + "/OutputData/Sequences";

        File directory = new File(directoryPath);

        BufferedReader br = null;
        HashMap<String, BufferedWriter> writerMap = new HashMap<String, BufferedWriter>(); //<prefix, writer>
        try {
            br = new BufferedReader(new FileReader(
                    System.getProperty("user.dir") + "/OutputData/part-r-00000"));

            while (br.readLine() != null) {
                String parts[] = br.readLine().split(" ");
                Float time = Float.parseFloat(parts[0]);
                String srcAdd = parts[1], dstAdd = parts[2], flow = parts[3];
                if (!writerMap.containsKey(srcAdd)) {
                    writerMap.put(srcAdd, new BufferedWriter(new FileWriter(
                            System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd)));
                } else {
                    writerMap.get(srcAdd).write(time + " " + dstAdd + " " + flow + "\n");
                }
            }
        } catch (Exception e) {
            System.out.println("Error when reading data in sequence processing: " + e.getMessage());
            e.printStackTrace();
            exit(1);
        }
    }


    public static void run() {
//        generateSequenceDatabase();
        sequenceMining();
    }
}

class SequenceMiningProcessor implements Callable<Void> {
    private File sequenceToMineFile;

    public SequenceMiningProcessor(File sequenceToMineFile) {
        this.sequenceToMineFile = sequenceToMineFile;
    }

    public Void call() throws Exception {
        String eventFile = System.getProperty("user.dir") + "/OutputData/Events/" + sequenceToMineFile.getParentFile().getName() + "/" + sequenceToMineFile.getName();
        BufferedWriter writer = new BufferedWriter(new FileWriter(eventFile));
        BufferedReader reader = new BufferedReader(new FileReader(sequenceToMineFile));
        int lines = 0;
        while (reader.readLine() != null) lines++;
        reader.close();
        System.out.println("Mining sequences from: " + sequenceToMineFile.getAbsolutePath());
        try {
            AlgoTKS algo = new AlgoTKS();
            algo.setMinimumPatternLength(1);
            algo.setMaximumPatternLength(20);
            // TODO: adjust
            algo.setMinsup(Math.max(4, (int) (0.2 * lines)));
            PriorityQueue<PatternTKS> patterns = algo.runAlgorithm(sequenceToMineFile.getAbsolutePath(),
                    System.getProperty("user.dir") + "/OutputData", 15);
//                        System.out.println("Number of patterns: " + patterns.size());
            int cnt = 0;
            while (!patterns.isEmpty()) {
                PatternTKS pattern = patterns.poll();
                // TODO: modify to get unique pattern, accumulate support from all file
//                            if (!eventSet.contains(pattern) && patterns.size() < k) eventSet.add(pattern);
//                            if (patterns.size() < k) writer.write(pattern.getPrefix() + " #SUP: " + (float) pattern.support/lines + "\n");
//                            cnt++;
//                            if (patterns.size() > k) break;
                writer.write(pattern.getPrefix() + " #SUP: " + pattern.support + "\n");
                if (++cnt > 15) break;
            }
            writer.flush();
            writer.close();
        } catch (Exception e) {
            System.out.println("Error when mining sequences: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }
}