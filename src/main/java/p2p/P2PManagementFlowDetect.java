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
                return length - o.length;
            } else if (compare !=0){
                return compare > 0 ? 1 : -1;
            } else {
                return 0;
            }
        }
    }

    static String sequenceFolder = System.getProperty("user.dir") + "/OutputData/Sequences/";

    static int lengthThreshold = Config.LengthThreshold;

    static int strengthThreshold = Config.StrengthThreshold;

    static String botnetListFilePath = System.getProperty("user.dir") + "/InputData/BotnetList.txt";

    static HashMap<String, Boolean> predictValue = new HashMap<String, Boolean>();

    static HashSet<String> botnetList = new HashSet<String>();


    public static void frequentBehaviourMining() {
        BufferedReader br = null;
        BufferedWriter writer = null;

        for (File folder : new File(sequenceFolder).listFiles()) {
            ArrayList<Pattern> patterns = new ArrayList<Pattern>();
            HashMap<Integer, Pattern> patternIDMap = new HashMap<Integer, Pattern>();
            System.out.println("Mining sequences for: " + folder.getName());

            try {
                br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/event.txt"));
                String line;
                while ((line = br.readLine()) != null) {
//                    System.out.println(line);
                    String[] parts = line.split(" #SUP: ");
                    float sup = Float.parseFloat(parts[1]);
                    String[] sizes = parts[0].split(" -1 ");
                    int cnt = parts[0].split(" -1 ").length - 1;
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

            Collections.sort(patterns);
            for (int i = patterns.size() - 1; i >= 0; i--) {
//                System.out.println(patterns.get(i).pattern + " " + patterns.get(i).sup + " " + patterns.get(i).length);
                patternIDMap.put(i, patterns.get(i));
            }

            try {
                // eventSequence: chuỗi danh sách index event trong patterns
                HashMap<String, ArrayList<Integer>> eventSequence = new HashMap<String, ArrayList<Integer>>();
                br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/all.txt"));
                String line;

                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(" ");
                    String dstAdd = parts[0], flow = parts[1];
                    for (int i = patterns.size() - 1; i >= 0; i--) {
                        if (flow.matches(patterns.get(i).pattern)) {
                            if (!eventSequence.containsKey(dstAdd)) {
                                eventSequence.put(dstAdd, new ArrayList<>());
                            }
                            eventSequence.get(dstAdd).add(i);
                            break;
                        }
                    }
                }

                writer = new BufferedWriter(new FileWriter(folder.getAbsolutePath() + "/behaviour.txt"));

                for (Map.Entry<String, ArrayList<Integer>> entry : eventSequence.entrySet()) {
                    String dstAdd = entry.getKey();
                    System.out.println("Event sequence database of: " + dstAdd);
//                    writer.write(dstAdd + " ");
                    ArrayList<Integer> events = entry.getValue();
                    for (int i = 0; i < events.size(); i++) {
                        System.out.printf("%d ", events.get(i));
                        writer.write(events.get(i) + " -1 ");
                    }
                    writer.write("-2\n");
                    System.out.println();
                }
                writer.flush();
            }  catch (Exception e) {
                System.out.println("Error when mining behaviour: " + e.getMessage());
                e.printStackTrace();
            }

            try {
                AlgoTKS algo = new AlgoTKS();
                algo.setMinimumPatternLength(2);
                algo.setMaximumPatternLength(20);
                PriorityQueue<PatternTKS> behaviourPatterns = algo.runAlgorithm(folder.getAbsolutePath() + "/behaviour.txt"
                        , folder.getAbsolutePath() + "/behaviourTKS.txt", 20);
                int numberOfBehaviours = getNumberOfLines(folder.getAbsolutePath() + "/behaviour.txt");
                System.out.println("Number of patterns: " + patterns.size());
                for (PatternTKS pattern : behaviourPatterns) {
                    String[] parts = pattern.getPrefix().split(" -1 ");

                    int behaviourLength = 0;
                    for (String part : parts) {
                        behaviourLength += patternIDMap.get(Integer.parseInt(part)).length;
                    }
                    float behaviourSupport = (float) pattern.support / numberOfBehaviours;
                    System.out.println("Behaviour length: " + behaviourLength);
                    predictValue.put(folder.getName(), behaviourLength > lengthThreshold ||
                            behaviourSupport * behaviourLength > strengthThreshold);
                    // TODO: modify to get result on certain criteria
                    break;

                }

            } catch (Exception e) {

            }
        }
        System.out.println("Predict value ");
        for (Map.Entry<String, Boolean> entry : predictValue.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    static void readBotnetList() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(botnetListFilePath));
            String line;
            while ((line = br.readLine()) != null) {
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
        for (Map.Entry<String, Boolean> entry : predictValue.entrySet()) {
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
