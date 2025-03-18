package p2p;

import Config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;
import spmf.AlgoTKS;
import spmf.PatternTKS;

import java.io.*;
import java.util.*;

public class P2PManagementFlowDetect {
    static class Pattern implements Comparable<Pattern> {
        String pattern;
        float sup;
        int length;
        int totalSize;

        public Pattern(String pattern, float sup, int length, int totalSize) {
            this.pattern = pattern;
            this.sup = sup;
            this.length = length;
            this.totalSize = totalSize;
        }

        public int compareTo(Pattern o) {
            float compare = this.sup - o.sup;
            if (length != o.length) {
                return o.length - length;
            } else if (compare !=0){
                return compare > 0 ? -1 : 1;
            } else {
                return 0;
            }
        }
    }

    static String sequenceFolder = System.getProperty("user.dir") + "/OutputData/Sequences/";

    static int lengthThreshold = Config.LengthThreshold;

    static double strengthThreshold = Config.StrengthThreshold;

    static String botnetListFilePath = System.getProperty("user.dir") + "/InputData/BotnetList.txt";

    static HashMap<String, Boolean> predictValues = new HashMap<String, Boolean>();

    static HashSet<String> botnetList = new HashSet<String>();

    static ArrayList<Pattern> patternMapping(File folder) {
        ArrayList<Pattern> patterns = new ArrayList<Pattern>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/event.txt"));
            String line;
            while ((line = br.readLine()) != null) {
//                    System.out.println(line);
                String[] parts = line.split(" #SUP: ");
                float sup = Float.parseFloat(parts[1]);
                String[] sizes = parts[0].split(" -1 ");
                int cnt = parts[0].split(" -1 ").length;
                int totalSize = 0;
                for ( String size : sizes) {
                    totalSize += Integer.parseInt(size);
                }
//                    System.out.println("Count: " + cnt);
                String regex = parts[0].substring(0, parts[0].lastIndexOf(" -1"));
                regex ="(.*?,)?" + regex.replace(" -1 ", ",(.*?,)?") + "(,.*?)?";
//                    System.out.println(regex);
                patterns.add(new Pattern(regex, (float) sup, cnt, totalSize));
            }
            br.close();
        } catch (Exception e) {
            System.out.println("Error when mining behaviour: " + e.getMessage());
            e.printStackTrace();
        }
        //Sort in reverse
        Collections.sort(patterns);
        return patterns;
    }


    public static void frequentBehaviourMining() {
        BufferedReader br = null;
        BufferedWriter writer = null;

        for (File folder : new File(sequenceFolder).listFiles()) {
            ArrayList<Pattern> patterns = new ArrayList<Pattern>();
            HashMap<Integer, Pattern> patternIDMap = new HashMap<Integer, Pattern>();
            System.out.println("Mining sequences for: " + folder.getName());

            patterns = patternMapping(folder);


            for (int i = patterns.size() - 1; i >= 0; i--) {
//                System.out.println(patterns.get(i).pattern + " " + patterns.get(i).sup + " " + patterns.get(i).length);
                patternIDMap.put(i, patterns.get(i));
            }

            try {
                // eventSequence: chuỗi danh sách index event trong patterns
                HashMap<String, ArrayList<Integer>> eventSequence = new HashMap<String, ArrayList<Integer>>();
                br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/all.txt"));
                String line;

                writer = new BufferedWriter(new FileWriter(folder.getAbsolutePath() + "/behaviour.txt"));
                // Create event sequence while mapping with destIP
                // No longer need if already mapping in previous step
                // TODO: handle no matching flows
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\.");
//                    String dstAdd = parts[0], flow = parts[1];
                    boolean matched = false;
                    for (String flow : parts) {
                        for (int i = 0; i < patterns.size(); i++) {
                            if (flow.matches(patterns.get(i).pattern)) {
                                writer.write(i + " -1 ");
                                matched = true;
                                break;
                            }
                        }
                    }
                    writer.write(matched ? "-2\n" : "");
                }


//                for (Map.Entry<String, ArrayList<Integer>> entry : eventSequence.entrySet()) {
//                    String dstAdd = entry.getKey();
//                    System.out.println("Event sequence database of: " + dstAdd);
////                    writer.write(dstAdd + " ");
//                    ArrayList<Integer> events = entry.getValue();
//                    for (int i = 0; i < events.size(); i++) {
//                        System.out.printf("%d ", events.get(i));
//                        writer.write(events.get(i) + " -1 ");
//                    }
//                    writer.write("-2\n");
//                    System.out.println();
//                }
                writer.flush();
            }  catch (Exception e) {
                System.out.println("Error when mining behaviour: " + e.getMessage());
                e.printStackTrace();
            }

            try {
                AlgoTKS algo = new AlgoTKS();
                algo.setMinimumPatternLength(1);
                algo.setMaximumPatternLength(10);
                PriorityQueue<PatternTKS> behaviourPatterns = algo.runAlgorithm(folder.getAbsolutePath() + "/behaviour.txt"
                        , folder.getAbsolutePath() + "/behaviourTKS.txt", 15);
                int numberOfBehaviours = getNumberOfLines(folder.getAbsolutePath() + "/behaviour.txt");

                System.out.println("Number of patterns: " + patterns.size());
                for (PatternTKS pattern : behaviourPatterns) {
                    String[] parts = pattern.getPrefix().split(" -1 ");

                    int behaviourLength = 0;
                    for (String part : parts) {
                        behaviourLength += patternIDMap.get(Integer.parseInt(part)).length;
                    }
                    float behaviourSupport = (float) pattern.support / numberOfBehaviours;
                    if (behaviourSupport < 0.5) {
                        continue;
                    }
                    System.out.println("Behaviour length: " + behaviourLength);

                    boolean predictValue = behaviourLength > lengthThreshold || behaviourSupport * behaviourLength > strengthThreshold;
                    if (predictValue) {
                        predictValues.put(folder.getName(), true);
                        break;
                    }
                    // TODO: modify to get result on certain criteria
                }
                if (!predictValues.containsKey(folder.getName())) {
                    predictValues.put(folder.getName(), false);
                }

            } catch (Exception e) {

            }
        }
        System.out.println("Predict value ");
        for (Map.Entry<String, Boolean> entry : predictValues.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    static void readBotnetList() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(botnetListFilePath));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                botnetList.add(line);
            }
            br.close();
        } catch (Exception e) {
            System.out.println("Error when reading botnet list: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static void evaluate() {
        int TP = 0, TN = 0, FP = 0, FN = 0;
        for (Map.Entry<String, Boolean> entry : predictValues.entrySet()) {
            if (entry.getValue()) {
                if (botnetList.contains(entry.getKey())) {
                    TP++;
                } else {
                    FP++;
                }
            } else {
                if (botnetList.contains(entry.getKey())) {
                    FN++;
                } else {
                    TN++;
                }
            }
        }

        System.out.println("TP: " + TP + " TN: " + TN + " FP: " + FP + " FN: " + FN);
    }

    static int getNumberOfLines(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        int count = 0;
        while (br.readLine() != null) {
            count++;
        }
        br.close();
        return count;
    }

    public static void  run() throws IllegalArgumentException, IOException {
        readBotnetList();

        frequentBehaviourMining();

        evaluate();
    }
}
