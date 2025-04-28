package Event;

import org.apache.hadoop.util.hash.Hash;
import p2p.P2PManagementFlowDetect;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import Config.Config;

public class EventSequenceGenerate {
    static final int N = Config.N;

    public static File sequenceFolder = new File(System.getProperty("user.dir") + "/OutputData/Sequences/");

    static java.util.regex.Pattern timewindowPattern = java.util.regex.Pattern.compile("seq(\\d+)\\.txt");

    private static Integer extractIndexFromFilename(String filename, java.util.regex.Pattern pattern) {
        Matcher matcher = pattern.matcher(filename);
        if (matcher.matches()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                System.out.println("Invalid number format in filename: " + filename);
            }
        }
        return null; // Return null if no valid index found
    }

    public static class Pattern implements Comparable<Pattern> {
        public String pattern;
        public float sup;
        public int length;
        public int totalSize;

        public Pattern(String pattern, float sup, int length, int totalSize) {
            this.pattern = pattern;
            this.sup = sup;
            this.length = length;
            this.totalSize = totalSize;
        }

        public int compareTo(Pattern o) {
            float supDiff = this.sup - o.sup;
            float lengthDiff = this.length - o.length;

            if (length != o.length) {
                return o.length - length;
            } else if (supDiff !=0){
                return supDiff > 0 ? -1 : 1;
            } else {
                return 0;
            }
//            return (int) -compare;
//            return -(int)(supDiff + 5*lengthDiff);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true; else return false;
//            if (!(o instanceof Pattern)) return false;
//            Pattern other = (Pattern) o;
//            return this.sup == other.sup;

        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern);
        }
    }

//    public static List<File> findFilesWithIndexInRange(String directoryPath, int min, int max) {
//        // Get all files
//        File[] files = folder.listFiles();
//        if (files == null) return Collections.emptyList();
//
//        // Regex pattern to extract index from filename
//        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("seq(\\d+)\\.txt");
//
//        return Arrays.stream(files)
//                .filter(File::isFile)
//                .filter(file -> {
//                    Integer index = extractIndexFromFilename(file.getName(), pattern);
//                    return index != null && index >= min && index <= max;
//                })
//                .collect(Collectors.toList());
//    }

    static ArrayList<EventSequenceGenerate.Pattern> eventPatternMapping(File eventFile) {
        ArrayList<EventSequenceGenerate.Pattern> patterns = new ArrayList<>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(eventFile.getAbsolutePath()));
            String line;
            while ((line = br.readLine()) != null) {
//                    System.out.println(line);
                String[] parts = line.split(" #SUP: ");
                float sup = Float.parseFloat(parts[1]);
                String[] sizes = parts[0].split(" -1 ");
                int cnt = parts[0].split(" -1 ").length;
                int totalSize = 0;
//                for ( String size : sizes) {
//                    totalSize += Integer.parseInt(size);
//                }
//                    System.out.println("Count: " + cnt);
//                String regex = parts[0].substring(0, parts[0].lastIndexOf(" -1"));
//                regex ="(?:[^,]*,)?" + regex.replace(" -1 ", ",(?:[^,]*,)?") + "(?:,[^,]*)?";
//                    System.out.println(regex);
                patterns.add(new Pattern(parts[0], (float) sup, cnt, totalSize));
            }
            br.close();
        } catch (Exception e) {
            System.out.println("Error when mining behaviour: " + e.getMessage());
            e.printStackTrace();
        }
        //Sort in reverse
//        Collections.sort(patterns);
        return patterns;
    }

    public static void eventSequenceGenerate() throws Exception {
        ArrayList<EventSequenceGenerate.Pattern> eventPatterns = null;
        BufferedWriter writer = null;
        File eventSequenceFolder = new File(System.getProperty("user.dir") + "/OutputData/EventSequences/");

        if (!eventSequenceFolder.exists()) {
            eventSequenceFolder.mkdir();
        }



        for (File hostSequenceFolder : sequenceFolder.listFiles()) {
            HashMap<String, ArrayList<Integer>> evenSequenceMap = new HashMap<String, ArrayList<Integer>>();
            HashMap<String, Pattern> patternMap = new HashMap<String, Pattern>();
            TreeSet<EventSequenceGenerate.Pattern> eventSet = new TreeSet<EventSequenceGenerate.Pattern>();
            String host = hostSequenceFolder.getName();
            File hostEventFolder = new File(System.getProperty("user.dir") + "/OutputData/Events/" + host + "/");
            BufferedReader br = null;

            File hostEventSequenceFolder = new File(System.getProperty("user.dir") + "/OutputData/EventSequences/" + host + "/"); //Folder
            if (!hostEventSequenceFolder.exists()) {
                hostEventSequenceFolder.mkdir();
            }

            File[] numberSortedSequenceFile = hostSequenceFolder.listFiles();
            if (numberSortedSequenceFile != null) {
                Arrays.sort(numberSortedSequenceFile, Comparator.comparingInt(f -> extractIndexFromFilename(f.getName(), timewindowPattern)));
            }

            File firstFile = numberSortedSequenceFile[0];
            int minIndex = extractIndexFromFilename(firstFile.getName(), timewindowPattern);
            int maxIndex = minIndex + N;

            File eventSequenceFile = new File(System.getProperty("user.dir") + "/OutputData/EventSequences/" + host + "/" + minIndex + ".txt");
            writer = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/OutputData/EventSequences/" + host + "/" + minIndex + ".txt"));

            patternMap = collectEventsInATimeWindow(hostEventFolder, minIndex, maxIndex);
            eventSet.addAll(patternMap.values());
            int cnt = 1;
            for (EventSequenceGenerate.Pattern pattern : eventSet) {
                writer.write("#" + cnt++ + ":" + pattern.pattern + "#LEN: " + pattern.length + "#SUP: " + pattern.sup + "\n");
            }
            writer.flush();

            for (File sequenceFile : numberSortedSequenceFile) {
                int index = extractIndexFromFilename(sequenceFile.getName(), timewindowPattern);
                if (index > maxIndex) {
                    printFromEventSequenceMap(evenSequenceMap, writer);
                    evenSequenceMap.clear();

                    minIndex = index;
                    maxIndex = minIndex + N;
                    patternMap = collectEventsInATimeWindow(hostEventFolder, minIndex, maxIndex);
                    eventSet.clear();
                    eventSet.addAll(patternMap.values());
                    writer.close();

                    eventSequenceFile = new File(System.getProperty("user.dir") + "/OutputData/EventSequences/" + host + "/" + minIndex + ".txt");
                    writer = new BufferedWriter(new FileWriter(eventSequenceFile.getAbsolutePath()));
                    cnt = 1;
                    for (EventSequenceGenerate.Pattern pattern : eventSet) {
                        writer.write("#" + cnt++ + ":" + pattern.pattern + "#LEN: " + pattern.length + "#SUP: " + pattern.sup + "\n");
                    }
                    writer.flush();
                }


                try {
                    br = new BufferedReader(new FileReader(sequenceFile.getAbsolutePath()));
                    String line = br.readLine();
                    while (line != null && !line.isEmpty()) {
                        String[] parts = line.split(" ");
                        String dstAdd = parts[0], flow = parts[1];
                        boolean isMatch = false;
                        for (String flowUnit : flow.split("\\.")) {
                            cnt = 1;
                            for (EventSequenceGenerate.Pattern pattern : eventSet) {
                                java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern.pattern);
                                Matcher m = p.matcher(flowUnit);

                                if (matchBySlideWindow(pattern.pattern,flowUnit)) {
                                    if (evenSequenceMap.containsKey(dstAdd)) {
                                        evenSequenceMap.get(dstAdd).add(cnt);
                                    } else {
                                        ArrayList<Integer> list = new ArrayList<Integer>();
                                        list.add(cnt);
                                        evenSequenceMap.put(dstAdd, list);
                                    }
//                                    writer.write(cnt + " -1 ");
                                    isMatch = true;
                                    break;
                                }
                                cnt++;
                            }

                        }
//                        if (isMatch) writer.write("-2\n");
                        writer.flush();
                        line = br.readLine();
                    }
                } catch (Exception e) {
                    System.out.println("Error when reading sequence file: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            printFromEventSequenceMap(evenSequenceMap, writer);
//            writer.close();
        }
    }

    static HashMap<String, Pattern> collectEventsInATimeWindow(File eventFolder, int minInd, int maxInd) throws Exception {
        HashMap<String, Pattern> patternMap = new HashMap<String, Pattern>();

        final int finalMinIndex = minInd;
        final int finalMaxIndex = maxInd;

        for (File eventFile : eventFolder.listFiles(file -> {
            Integer eventIndex = extractIndexFromFilename(file.getName(), timewindowPattern);
            return eventIndex != null && eventIndex >= finalMinIndex && eventIndex <= finalMaxIndex;
        })) {
            ArrayList<EventSequenceGenerate.Pattern> patterns = eventPatternMapping(eventFile);
            for (EventSequenceGenerate.Pattern pattern : patterns) {
                if (!patternMap.containsKey(pattern.pattern)) {
                    patternMap.put(pattern.pattern, pattern);
                } else {
                    Pattern existingPattern = patternMap.get(pattern.pattern);
                    existingPattern.sup += pattern.sup;
                }
            }
        }
        return patternMap;
    }

    static void printFromEventSequenceMap(HashMap<String, ArrayList<Integer>> eventSequenceMap, BufferedWriter writer) {
        try {
            for (Map.Entry<String, ArrayList<Integer>> entry : eventSequenceMap.entrySet()) {
                for (int i = 0; i < entry.getValue().size(); i++) {
                    writer.write(entry.getValue().get(i) + " -1 ");
                }
                if (!entry.getValue().isEmpty()) writer.write("-2\n");
            }
            writer.flush();
//            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static boolean matchBySlideWindow(String pattern, String line) {
//        ArrayList<String> timeIntervalResult = new ArrayList<>();
        StringTokenizer lineTokenizer = new StringTokenizer(line, ",");
        String[] lineParts = line.split(","), patternParts = pattern.split(" -1 "),
                timeParts = lineParts[1].split(" "), parts = lineParts[0].split(" -1 ");
        int patternI = 0;
//        HashSet<String> packetSet = new HashSet<String>();
        //packetSet.addAll(List.of(patternParts[patternI].split(" ")));
        while (lineTokenizer.hasMoreTokens()) {
            String packetSize = lineTokenizer.nextToken();
            if (packetSize.equals(patternParts[patternI])) {
                patternI++;
                if (patternI >= patternParts.length) {
                    return true;
                }
            }
            if (patternI >= patternParts.length) {
                return true;
            }
        }
        if (patternI >= patternParts.length) {
            return true;
        }
        return false;
    }

    public static void run() {
        try {
            eventSequenceGenerate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}