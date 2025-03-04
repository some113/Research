package Behaviour;

import Config.Config;
import p2p.P2PManagementFlowDetect;
import spmf.AlgoTKS;
import spmf.PatternTKS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.PriorityQueue;

public class BehaviourExtract {
    private static File eventSequenceFolder = new File(
            System.getProperty("user.dir") + "/OutputData/EventSequences");

    static HashMap<String, Boolean> predictValue = new HashMap<String, Boolean>();

    static int lengthThreshold = Config.LengthThreshold;

    static int strengthThreshold = Config.StrengthThreshold;


    static class Pattern implements Comparable<BehaviourExtract.Pattern> {
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

        public int compareTo(BehaviourExtract.Pattern o) {
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

    static void readBotnetList() {

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

    static void behaviourExtract() {
        for (File file : eventSequenceFolder.listFiles()) {
            try {
                AlgoTKS algo = new AlgoTKS();
                algo.setMinimumPatternLength(2);
                algo.setMaximumPatternLength(10);
                PriorityQueue<PatternTKS> behaviourPatterns = algo.runAlgorithm(file.getAbsolutePath()
                        , System.getProperty("user.dir") + "/OutputData/Behaviour/" + file.getName() + ".txt", 5);
                int numberOfBehaviours = getNumberOfLines(file.getAbsolutePath());
//                System.out.println("Number of patterns: " + patterns.size());
                for (PatternTKS pattern : behaviourPatterns) {
                    String[] parts = pattern.getPrefix().split(" -1 ");

                    int behaviourLength = 0;
                    for (String part : parts) {
                        behaviourLength += Integer.parseInt(part); // patternIDMap.get(Integer.parseInt(part)).length;
                    }
                    float behaviourSupport = (float) pattern.support / numberOfBehaviours;
//                    System.out.println("Behaviour length: " + behaviourLength);
                    predictValue.put(String.join(".", Arrays.copyOfRange(file.getName().split("\\."),
                            0, 4)), (behaviourLength > lengthThreshold &&behaviourSupport > 0.5)
                            || behaviourSupport * behaviourLength > strengthThreshold);
//                    // TODO: modify to get result on certain criteria
                    break;
                }

            } catch (Exception e) {

            }
        }

    }


    public static void run() throws Exception {
        behaviourExtract();

        System.out.println("Predict value:");
        for (String key : predictValue.keySet()) {
            if (predictValue.get(key)) System.out.println(key + " " + predictValue.get(key));
        }
    }
}
