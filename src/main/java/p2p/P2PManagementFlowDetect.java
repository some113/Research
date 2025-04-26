package p2p;

import Config.Config;
import Event.EventSequenceGenerate.Pattern;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import spmf.AlgoTKS;
import spmf.PatternTKS;

import javax.imageio.ImageIO;
import java.awt.*;

import java.awt.image.BufferedImage;
import java.io.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;

import static p2p.P2PManagementFlowDetect.*;
import Thread.MyMultiThread;

public class P2PManagementFlowDetect {


    static String eventSequenceFolder = System.getProperty("user.dir") + "/OutputData/EventSequences/";

    static int lengthThreshold = Config.LengthThreshold;

    static double strengthThreshold = Config.StrengthThreshold;

    static String botnetListFilePath = System.getProperty("user.dir") + "/InputData/BotnetList.txt";

    public static HashMap<String, Boolean> predictValues = new HashMap<String, Boolean>();

    static HashSet<String> botnetList = new HashSet<String>();

    static HashMap<String, ArrayList<String>> resultMap = new HashMap<String,  ArrayList<String>>();

    public static PatternStatisticVisualize visualize = new PatternStatisticVisualize();

    public static HashMap<String, Boolean> globalIsBotnet = new HashMap<String, Boolean>();

    public static void frequentBehaviourMining() {
        BufferedReader br = null;
        BufferedWriter writer = null;
        ExecutorService executor = Executors.newFixedThreadPool(1);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        MyMultiThread myMultiThread = new MyMultiThread(10 * 60 * 1000, 5);

        XYSeries points;
        for (File folder : new File(eventSequenceFolder).listFiles()) {
            //if (folder.getName().startsWith("147.32.85") || folder.getName().startsWith("147.32.86")) continue;
            boolean isBonet = false;
            points = new XYSeries(folder.getName());
//            visualize.addChartStatistic(points, folder.getName());
            visualize.addPlaceHolder(folder.getName());

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
//                CompletableFuture<?> future = CompletableFuture.runAsync(new frequentBehaviourProcessor(fileByTimeWindow), executor);
//                future = future.completeOnTimeout(null, 1, TimeUnit.MINUTES);
//                futures.add(future);
                myMultiThread.addTask(new frequentBehaviourProcessor(fileByTimeWindow));
//                patternIDMap = buildPatternIDMap(fileByTimeWindow);
//                try {
//                    int numberOfBehaviours = getNumberOfLines(fileByTimeWindow.getAbsolutePath()) - patternIDMap.size();
//                    int minSupValue = Math.max((int)(0.2 * numberOfBehaviours), 10);
//
//                    AlgoTKS algo = new AlgoTKS();
//                    algo.setMinimumPatternLength(1);
//                    algo.setMaximumPatternLength(10);
//                    algo.setMinsup((int)0.3 * numberOfBehaviours);
//                    PriorityQueue<PatternTKS> behaviourPatterns = algo.runAlgorithm(fileByTimeWindow.getAbsolutePath()
//                            , folder.getAbsolutePath() + "/behaviourTKS.txt", 15);
////                    algo = new AlgoTKS();
////                    algo.setMinimumPatternLength(10);
////                    algo.setMinsup((int)(0.01 * numberOfBehaviours));
//                    behaviourPatterns.addAll(algo.runAlgorithm(fileByTimeWindow.getAbsolutePath()
//                            , folder.getAbsolutePath() + "/behaviourTKS.txt", 15));
//
////                    System.out.println("Number of patterns: " + patterns.size());
//                    if (!behaviourPatterns.isEmpty()) System.out.println("Pattern of " + folder.getName() + " with their sup: ");
//                    while (!behaviourPatterns.isEmpty()) {
//                        PatternTKS pattern = behaviourPatterns.poll();
//                        String[] parts = pattern.getPrefix().split(" -1 ");
//
//                        int behaviourLength = 0;
//                        for (String part : parts) {
//                            behaviourLength += patternIDMap.get(Integer.parseInt(part)).length;
//                        }
//                        float behaviourSupport = (float) pattern.support / numberOfBehaviours;
////                        if (behaviourSupport < 0.5) {
////                            continue;
////                        }
//                        System.out.print(behaviourLength + "-" + behaviourSupport + " ");
////                        System.out.println("Behaviour length: " + behaviourLength);
//                        points.add(behaviourLength, behaviourSupport * behaviourLength);
//
//                        boolean predictValue = behaviourLength > lengthThreshold || behaviourSupport * behaviourLength > strengthThreshold;
//                        if (predictValue) {
//                            predictValues.put(folder.getName(), true);
//                            isBonet = true;
////                            break;
//                        }
//                        // TODO: modify to get result on certain criteria
//                    }
//
//                } catch (Exception e) {
//
//                }
//                if (isBonet) {
//                    break;
//                }

            }
        }

//        for (Future<?> future : futures) {
//            try {
//                future.get(30, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                System.out.println("Timeout when mining behaviour: ");
//                e.printStackTrace();
//            } finally {
//                future.cancel(true);
//            }
//        }
//        try {
//            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } catch (ExecutionException e) {
//            throw new RuntimeException(e);
//        }
//        for (Future<?> future : futures) {
//            future.cancel(true);
//        }
//        executor.shutdown();

        MyMultiThread.run();

        for (File folder : new File(eventSequenceFolder).listFiles()) {
            if (!predictValues.containsKey(folder.getName())) {
                predictValues.put(folder.getName(), false);
            }
        }
        System.out.println("Predict value ");
        for (Map.Entry<String, Boolean> entry : predictValues.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    public static HashMap<Integer, Pattern> buildPatternIDMap(File file) {
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

    public static int getNumberOfLines(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        int count = 0;
        while (br.readLine() != null) {
            count++;
        }
        br.close();
        return count;
    }
    private static JFreeChart createChart(String title, double[] xData, double[] yData) {
        XYSeries series = new XYSeries(title);
        for (int i = 0; i < xData.length; i++) {
            series.add(xData[i], yData[i]);
        }
        XYSeriesCollection dataset = new XYSeriesCollection(series);
        return ChartFactory.createXYLineChart(title, "X", "Y", dataset);
    }


    public static void  run() throws IllegalArgumentException, IOException {
        readBotnetList();

        frequentBehaviourMining();

        evaluate();

        visualize.save();
    }
}
class PatternStatisticVisualize {
    ArrayList<JFreeChart> charts = new ArrayList<JFreeChart>();
//    static HashMap<String, XYSeries> pointsMap= new HashMap<String, XYSeries>();
    static XYSeriesCollection pointsMap = new XYSeriesCollection();
    //ArrayList<String> allowedHosts = new ArrayList<String>();
    String[] allowedHost = {"172.16.0.2", "172.16.0.11", "172.16.0.12", "172.16.2.11", "172.16.2.12", "147.32.84.165",
            "147.32.84.191", "147.32.84.192", "192.168.1.126", "10.0.2.113", "10.0.2.104", "10.0.2.106", "10.0.2.103",
            "192.168.0.9", "192.168.0.250", "192.168.0.251", "192.168.0.150", "192.168.0.151", "192.168.1.2",
            "192.168.2.2", "192.168.3.2", "192.168.4.2"};

    public static void addPoint(String host, float x, float y) {
        if (pointsMap.getSeriesIndex(host) != -1) {
            XYSeries points = pointsMap.getSeries(host);
            points.add(x, y);
        } else {
            XYSeries points = new XYSeries(host);
            points.add(x, y);
            pointsMap.addSeries(points);
        }
    }

    public static void addPlaceHolder(String host) {
        XYSeries points = new XYSeries(host);
        pointsMap.addSeries(points);
    }

    void addChartStatistic(XYSeries points, String title) {
//        if (!allowedHostSet.contains(title)) {
//            return;
//        }
        XYSeriesCollection dataset = new XYSeriesCollection(points);


        JFreeChart chart = ChartFactory.createScatterPlot(
                title, "X", "Y", dataset, PlotOrientation.VERTICAL, false, false, false );

        XYPlot plot = chart.getXYPlot();
        NumberAxis rangeAxis = (NumberAxis)plot.getRangeAxis();
        NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
        rangeAxis.setRange(0, 10);
        domainAxis.setRange(0,80);
        charts.add(chart);
    }
    void save() {
        for (Object points : pointsMap.getSeries()) {
            points = (XYSeries) points;
//            XYSeriesCollection dataset = new XYSeriesCollection(pointsMap.get(host));
            JFreeChart chart = ChartFactory.createScatterPlot(
                    (String)((XYSeries) points).getKey(), "X", "Y", new XYSeriesCollection((XYSeries) points), PlotOrientation.VERTICAL, false, false, false
            );
            XYPlot plot = chart.getXYPlot();
            NumberAxis rangeAxis = (NumberAxis)plot.getRangeAxis();
            NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
            rangeAxis.setRange(0, 10);
            domainAxis.setRange(0,80);
            charts.add(chart);
        }

        int edgeNumber = (int) Math.ceil(Math.sqrt(charts.size()));
        int chartWidth = 800, chartHeight = 600;
        int totalWidth = chartWidth * edgeNumber, totalHeight = chartHeight * edgeNumber;

        BufferedImage combinedImage = new BufferedImage(totalWidth, totalHeight, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = combinedImage.createGraphics();



        for (int i = 0; i < charts.size(); i++) {
            BufferedImage image = charts.get(i).createBufferedImage(chartWidth, chartHeight);
            g.drawImage(image, (i % edgeNumber) * chartWidth, (i / edgeNumber) * chartHeight, null);
            g.drawImage(image, 0, 0, null);
        }

        g.dispose();

        try {
            ImageIO.write(combinedImage, "png", new File(System.getProperty("user.dir") + "/OutputData/chart.png"));
        } catch (IOException e) {
            System.out.println("Error when saving chart: " + e.getMessage());
            e.printStackTrace();
        }

    }


}

class frequentBehaviourProcessor implements Callable<Void> {
    private File eventSequenceFile;

    public frequentBehaviourProcessor(File eventSequenceFile) {
        this.eventSequenceFile = eventSequenceFile;
    }

    public Void call() {
//            if (globalIsBotnet.get(eventSequenceFile.getParentFile().getName())) {
//                return null;
//            }
        System.out.println("Mining frequent behaviour from: " + eventSequenceFile.getName());
        HashMap<Integer, Pattern> patternIDMap = buildPatternIDMap(eventSequenceFile);
        try {
            int numberOfBehaviours = getNumberOfLines(eventSequenceFile.getAbsolutePath()) - patternIDMap.size();
            int minSupValue = Math.max((int)(0.2 * numberOfBehaviours), 10);

            AlgoTKS algo = new AlgoTKS();
            algo.setMinimumPatternLength(1);
            algo.setMaximumPatternLength(10);
            algo.setMinsup((int)0.3 * numberOfBehaviours);
            PriorityQueue<PatternTKS> behaviourPatterns = algo.runAlgorithm(eventSequenceFile.getAbsolutePath()
                    , System.getProperty("user.dir") + "/OutputData/behaviourTKS.txt", 15);
            algo = new AlgoTKS();
            algo.setMinimumPatternLength(10);
            algo.setMinsup(5);
            behaviourPatterns.addAll(algo.runAlgorithm(eventSequenceFile.getAbsolutePath()
                , eventSequenceFile.getParentFile().getAbsolutePath() + "/behaviourTKS.txt", 15));

//                    System.out.println("Number of patterns: " + patterns.size());
            if (!behaviourPatterns.isEmpty()) System.out.println("Pattern of " + eventSequenceFile.getParentFile().getName() + " with their sup: ");
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
                System.out.print(behaviourLength + "-" + behaviourSupport + " ");
//                        System.out.println("Behaviour length: " + behaviourLength);
                PatternStatisticVisualize.addPoint(eventSequenceFile.getParentFile().getName(),
                        behaviourLength, behaviourSupport * behaviourLength);

                boolean predictValue = behaviourLength > P2PManagementFlowDetect.lengthThreshold || behaviourSupport * behaviourLength > strengthThreshold;
                if (predictValue) {
                    predictValues.put(eventSequenceFile.getParentFile().getName(), true);
                    globalIsBotnet.put(eventSequenceFile.getParentFile().getName(), true);
//                            break;
                }
                // TODO: modify to get result on certain criteria
            }

        } catch (Exception e) {
            System.out.println("Error when mining frequent behaviour " + eventSequenceFile.getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }
}