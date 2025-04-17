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

    static int BEHAVIOUR_DD_THREASHOLD = Config.BEHAVIOUR_DD_THRESHOLD;

    static int START_TIME_WINDOW = Config.START_TIME_WINDOW;

    public static void frequentBehaviourMining() {
        BufferedReader br = null;
        BufferedWriter writer = null;

        for (File folder : new File(eventSequenceFolder).listFiles()) {
            File timeIntervalFolder = new File(System.getProperty("user.dir") + "/OutputData/TimeIntervals/" + folder.getName());
            if (!timeIntervalFolder.exists()) {
                timeIntervalFolder.mkdir();
            }

            boolean isBonet = false;

//            ArrayList<Pattern> patterns = new ArrayList<Pattern>();
            HashMap<Integer, Pattern> patternIDMap = new HashMap<Integer, Pattern>();
//            System.out.println("Mining sequences for: " + folder.getName());

            for (File fileByTimeWindow : folder.listFiles()) {
                patternIDMap = buildPatternIDMap(fileByTimeWindow);
                File timeIntervalFile = new File(timeIntervalFolder.getAbsolutePath() + "/" + fileByTimeWindow.getName());
                try {
                    BufferedWriter timeIntervalWriter = new BufferedWriter(new FileWriter(timeIntervalFile));
                } catch (IOException e) {
                    System.out.println("Error when creating time interval file: " + e.getMessage());
                    e.printStackTrace();
                }
                try {
                    int numberOfBehaviours = getNumberOfLines(fileByTimeWindow.getAbsolutePath()) - patternIDMap.size();
                    int minSupValue = Math.max((int)(0.5 * numberOfBehaviours), 10);

                    AlgoTKS algo = new AlgoTKS();
                    algo.setMinimumPatternLength(1);
                    algo.setMaximumPatternLength(10);
                    algo.setMinsup(Math.max(10, (int)(0.4 * numberOfBehaviours)));
                    PriorityQueue<PatternTKS> behaviourPatterns = algo.runAlgorithm(fileByTimeWindow.getAbsolutePath()
                            , folder.getAbsolutePath() + "/behaviourTKS.txt", 15);
                    algo = new AlgoTKS();
                    algo.setMinimumPatternLength(10);
                    algo.setMinsup(10);
                    behaviourPatterns.addAll(algo.runAlgorithm(fileByTimeWindow.getAbsolutePath()
                            , folder.getAbsolutePath() + "/behaviourTKS.txt", 15));

//                    System.out.println("Number of patterns: " + patterns.size());
                    while (!behaviourPatterns.isEmpty()) {
                        PatternTKS pattern = behaviourPatterns.poll();
                        String[] parts = pattern.getPrefix().split(" -1 ");


                        int behaviourLength = 0;
                        for (String part : parts) {
                            behaviourLength += patternIDMap.get(Integer.parseInt(part)).length;
                        }
                        float behaviourSupport = (float) pattern.support / numberOfBehaviours;
//                        if (behaviourSupport < 0.5) {
//                            continue;
//                        }
                        System.out.println("Behaviour length: " + behaviourLength);

//                        boolean predictValue = behaviourLength > lengthThreshold || behaviourSupport * behaviourLength > strengthThreshold;
//                        if (predictValue) {
//                            predictValues.put(folder.getName(), true);
//                            isBonet = true;
//                            break;
//                        }

                        if (behaviourLength >= 3 && behaviourLength * getDDOfBehaviours(pattern, fileByTimeWindow) >= 300) {
                            predictValues.put(folder.getName(), true);
                            isBonet = true;
                            break;
                        };
                    }

                } catch (Exception e) {

                }
                if (isBonet) {
                    break;
                }

            }
            if (!predictValues.containsKey(folder.getName())) {
                predictValues.put(folder.getName(), false);
            }
        }
        System.out.println("Predict value ");
        for (Map.Entry<String, Boolean> entry : predictValues.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    static HashMap<Integer, Pattern> buildPatternIDMap(File file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
            HashMap<Integer, Pattern> patternIDMap = new HashMap<Integer, Pattern>();
            String line = br.readLine();
            while (line != null && line.startsWith("#")) {
                String[] parts = line.split("#");
                int patternLength = Integer.parseInt(parts[2].split("LEN: ")[1]);
                float patternSup = Float.parseFloat(parts[3].split("SUP: ")[1]);
                String patternRegex = parts[1].substring(parts[1].indexOf(":")+1);
                int patternInd = Integer.parseInt(parts[1].split(":")[0]);
                Pattern pattern = new Pattern(patternRegex, patternSup, patternLength, patternLength);

                patternIDMap.put(patternInd, pattern);
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

    /**
     * Temporaryly return number of diversity
     * @param pattern
     * @param eventSequenceFile
     */
    static int getDDOfBehaviours(PatternTKS pattern, File eventSequenceFile) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(eventSequenceFile.getAbsolutePath()));
            int cnt = 0;

            String host = eventSequenceFile.getParentFile().getName();
            System.out.println("At host: " + host + "\nStart time: ");
            ArrayList<Integer> startTimes = new ArrayList<Integer>();
            BufferedWriter writer = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/OutputData/TimeIntervals/" + host + "/" + eventSequenceFile.getName(), true));
            String line = br.readLine();
            while (line != null) {
                if (line.startsWith("#")) {
                    line = br.readLine();
                    continue;
                }
                String[] parts = line.split(" -1 ");
                ArrayList<Integer> timeIntervals = matchBySlideWindowAndGetTimeIntervals(pattern, line);
                if (timeIntervals != null) {
                    writer.write("Match for pattern: " + pattern.getPrefix() + "\n");
                    for (Integer timeInterval : timeIntervals) {
                        writer.write(timeInterval + " ");
                    }
                    writer.write("\n");
                    cnt++;
                    startTimes.add(timeIntervals.get(0));
                    System.out.print(timeIntervals.get(0) + " ");

                } else {
//                    System.out.println("No match");
                }
                line = br.readLine();
            }
            writer.flush();
            writer.close();

            System.out.println("\nNumber of match for pattern: " + pattern.getPrefix() + " is: " + cnt + " with start time: ");
            for (Integer startTime : startTimes) {
                System.out.print(startTime + " ");
            }
            System.out.println();

            int DD = getDDFromStartTimes(startTimes, START_TIME_WINDOW);
            System.out.println("DD of pattern: " + pattern.getPrefix() + " is: " + DD);
            return DD;
        } catch (Exception e) {
            System.out.println("Error when getting interval of behaviours: " + e.getMessage());
            e.printStackTrace();
        }
        return 0;
    }

    static ArrayList<Integer> matchBySlideWindowAndGetTimeIntervals(PatternTKS behaviourPattern, String line) {
        ArrayList<Integer> timeIntervalResult = new ArrayList<>();
        String[] lineParts = line.split("#"), behaviours = behaviourPattern.getPrefix().split(" -1 "),
                timeParts = lineParts[1].split(" "), parts = lineParts[0].substring(0, lineParts[0].indexOf("-2")).split(" -1 ");
        int behaviourI = 0, lineI = 0, lastTime = 0;
        for (;lineI < parts.length; lineI++) {

            String behaviour = behaviours[behaviourI];
            if (behaviour.equals(parts[lineI])) {
                if (timeIntervalResult.size() > 0) {
                    Integer interval = Integer.parseInt(timeParts[lineI]) - lastTime;
                    lastTime = Integer.parseInt(timeParts[lineI]);
                    timeIntervalResult.add(interval);
                } else {
                    lastTime = Integer.parseInt(timeParts[lineI]);
                    timeIntervalResult.add(Integer.parseInt(timeParts[lineI]));
                }
                behaviourI++;
                if (behaviourI >= behaviours.length) {
                    return timeIntervalResult;
                }
            }
            if (behaviourI >= behaviours.length) {
                return timeIntervalResult;
            }
        }
        if (behaviourI >= behaviours.length) {
            return timeIntervalResult;
        }
        return null;
    }

    static int getDDFromStartTimes(ArrayList<Integer> startTimes, int timeWindow) {
        int res = 0, cnt = 0, i = 0, j = 0;
        while (j < startTimes.size()) {
            if (startTimes.get(j) - startTimes.get(i) <= timeWindow) {
                cnt++;
            } else {
                while (startTimes.get(j) - startTimes.get(i) > timeWindow) {
                    i++;
                    cnt--;
                }
            }
            j++;
            res = Math.max(res, cnt);
        }
        return res;
    }

    public static void  run() throws IllegalArgumentException, IOException {
        File timeIntervalFile = new File(System.getProperty("user.dir") + "/OutputData/TimeIntervals");
        if (!timeIntervalFile.exists()) {
            timeIntervalFile.mkdir();
        }


        readBotnetList();

        frequentBehaviourMining();

        evaluate();
    }
}
