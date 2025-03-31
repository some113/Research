package p2p;

import Config.Config;
import Event.EventSequenceGenerate.Pattern;
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


    static String eventSequenceFolder = System.getProperty("user.dir") + "/OutputData/EventSequences/";

    static int lengthThreshold = Config.LengthThreshold;

    static double strengthThreshold = Config.StrengthThreshold;

    static String botnetListFilePath = System.getProperty("user.dir") + "/InputData/BotnetList.txt";

    static HashMap<String, Boolean> predictValues = new HashMap<String, Boolean>();

    static HashSet<String> botnetList = new HashSet<String>();

    static HashMap<String, ArrayList<String>> resultMap = new HashMap<String,  ArrayList<String>>();



    public static void frequentBehaviourMining() {
        BufferedReader br = null;
        BufferedWriter writer = null;

        for (File folder : new File(eventSequenceFolder).listFiles()) {

//            ArrayList<Pattern> patterns = new ArrayList<Pattern>();
            HashMap<Integer, Pattern> patternIDMap = new HashMap<Integer, Pattern>();
//            System.out.println("Mining sequences for: " + folder.getName());
            {
////            patterns = patternMapping(folder);
//
//
//            for (int i = patterns.size() - 1; i >= 0; i--) {
////                System.out.println(patterns.get(i).pattern + " " + patterns.get(i).sup + " " + patterns.get(i).length);
//                patternIDMap.put(i, patterns.get(i));
//            }
//
//            try {
//                // eventSequence: chuỗi danh sách index event trong patterns
////                HashMap<String, ArrayList<Integer>> eventSequence = new HashMap<String, ArrayList<Integer>>();
////                br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/all.txt"));
////                String line;
////
////                writer = new BufferedWriter(new FileWriter(folder.getAbsolutePath() + "/behaviour.txt"));
////                // Create event sequence while mapping with destIP
////                // No longer need if already mapping in previous step
////                // TODO: handle no matching flows
////                while ((line = br.readLine()) != null) {
////                    String[] parts = line.split("\\.");
//////                    String dstAdd = parts[0], flow = parts[1];
////                    boolean matched = false;
////                    for (String flow : parts) {
////                        for (int i = 0; i < patterns.size(); i++) {
////                            if (flow.matches(patterns.get(i).pattern)) {
////                                writer.write(i + " -1 ");
////                                matched = true;
////                                break;
////                            }
////                        }
////                    }
////                    writer.write(matched ? "-2\n" : "");
////                }
//
//
////                for (Map.Entry<String, ArrayList<Integer>> entry : eventSequence.entrySet()) {
////                    String dstAdd = entry.getKey();
////                    System.out.println("Event sequence database of: " + dstAdd);
//////                    writer.write(dstAdd + " ");
////                    ArrayList<Integer> events = entry.getValue();
////                    for (int i = 0; i < events.size(); i++) {
////                        System.out.printf("%d ", events.get(i));
////                        writer.write(events.get(i) + " -1 ");
////                    }
////                    writer.write("-2\n");
////                    System.out.println();
////                }
//                writer.flush();
//            }  catch (Exception e) {
//                System.out.println("Error when mining behaviour: " + e.getMessage());
//                e.printStackTrace();
//            }
            }

            for (File fileByTimeWindow : folder.listFiles()) {
                patternIDMap = buildPatternIDMap(fileByTimeWindow);
                try {
                    AlgoTKS algo = new AlgoTKS();
                    algo.setMinimumPatternLength(1);
                    algo.setMaximumPatternLength(10);
                    PriorityQueue<PatternTKS> behaviourPatterns = algo.runAlgorithm(fileByTimeWindow.getAbsolutePath()
                            , folder.getAbsolutePath() + "/behaviourTKS.txt", 15);
                    int numberOfBehaviours = getNumberOfLines(fileByTimeWindow.getAbsolutePath());

//                    System.out.println("Number of patterns: " + patterns.size());
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
    }

    static HashMap<Integer, Pattern> buildPatternIDMap(File file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
            HashMap<Integer, Pattern> patternIDMap = new HashMap<Integer, Pattern>();
            String line = br.readLine();
            while (line != null && !line.startsWith("#")) {
                String[] parts = line.split("#");
                int patternLength = Integer.parseInt(parts[2].split("LEN: ")[1]);
                float patternSup = Float.parseFloat(parts[3].split("SUP: ")[1]);
                Pattern pattern = new Pattern(parts[0], patternSup, patternLength, patternLength);

                patternIDMap.put(Integer.parseInt(parts[0].split(":")[0]), pattern);
                line = br.readLine();
            }
            br.close();
            return patternIDMap;
        } catch (Exception e) {
            System.out.println("Error when building pattern ID map: " + e.getMessage());
            e.printStackTrace();
            return null;
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
        resultMap.put("TP", new ArrayList<String>());
        resultMap.put("TN", new ArrayList<String>());
        resultMap.put("FP", new ArrayList<String>());
        resultMap.put("FN", new ArrayList<String>());
        String res;
        for (Map.Entry<String, Boolean> entry : predictValues.entrySet()) {
            if (entry.getValue()) {
                if (botnetList.contains(entry.getKey())) {
                    res = "TP";
                } else {
                    res = "FP";
                }
            } else {
                if (botnetList.contains(entry.getKey())) {
                    res = "FN";
                } else {
                    res = "TN";
                }
            }
            resultMap.get(res).add(entry.getKey());
        }
        for (Map.Entry<String, ArrayList<String>> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().size());
        }
        try {

            BufferedWriter writer = new BufferedWriter(new FileWriter(
                    new File(System.getProperty("user.dir") + "/OutputData/finalPredict.txt"), true
            ));
            for (Map.Entry<String, ArrayList<String>> entry : resultMap.entrySet()) {
                writer.write(entry.getKey() + "\n");
                for (String key : entry.getValue()) {
                    writer.write("\t" + key+ "\n");
                }
            }
            writer.flush();
            writer.close();
        } catch (Exception e) {
            System.out.println("Error when writing final predict result: " + e.getMessage());
        }
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
