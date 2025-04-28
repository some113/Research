package sequence;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.List;
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
import Thread.MyMultiThread;

import javax.imageio.ImageIO;

import static java.lang.System.exit;

public class SequenceDatabase {
    public static Heatmap botHeatmap = new Heatmap();
    public static Heatmap legitHeatmap = new Heatmap();
    public static HashSet<String> legitList = new HashSet<String>(Arrays.asList("192.168.1.2", "192.168.2.2", "192.168.3.2",
            "192.168.4.2", "192.168.11.2", "192.168.12.2", "192.168.13.2", "192.168.21.2", "192.168.22.2", "192.168.23.2",
            "192.168.31.2", "192.168.32.2", "192.168.33.2", "192.168.41.2", "192.168.42.2", "192.168.43.2"));
    public static HashSet<String> omitList = new HashSet<String>(Arrays.asList("172.16.2.112", "172.16.2.113", "172.16.2.13",
            "172.16.2.14", "172.16.2.2", "172.16.2.3"));



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
//        ExecutorService executor = Executors.newFixedThreadPool(5);
//        List<CompletableFuture<?>> futures = new LinkedList<>();
        List<Future<?>> futures = new LinkedList<>();
        List<Callable<Void>> tasks = new ArrayList<>();
        MyMultiThread myMultiThread = new MyMultiThread(10 * 60 * 1000, 5);

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
//                CompletableFuture<?> future = CompletableFuture.runAsync( new SequenceMiningProcessor(seqFile), executor);
//                future = future.completeOnTimeout(null, 10, TimeUnit.MINUTES);
//                Future<?> future = executor.submit(new SequenceMiningProcessor(seqFile));
//                futures.add(future);

                Callable<Void> task = new SequenceMiningProcessor(seqFile);
                myMultiThread.addTask(task);
            }



//                writer.flush();
//                writer.close();
        }

//        for (Future<?> future : futures) {
//            try {
//                future.get(10, TimeUnit.MINUTES);
//            } catch (ExecutionException e) {
//                System.out.println("Error when mining sequence: " + e.getMessage());
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                System.out.println("Timeout when mining sequence: " + e.getMessage());
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                System.out.println("Interrupted when mining sequence: " + e.getMessage());
//                e.printStackTrace();
//            }
//        }

//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//
//
//        Iterator<Future<?>> iter = futures.iterator();
//        while (iter.hasNext()) {
//            Future<?> future = iter.next();
//            future.cancel(true);
//        }
//        executor.shutdownNow();
            MyMultiThread.run();
//        try {
//            SequenceDatabase.legitHeatmap.saveImage(System.getProperty("user.dir") + "/OutputData/LegitHeatmap.png");
//            botHeatmap.saveImage(System.getProperty("user.dir") + "/OutputData/botHeatmap.png");
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.out.println("Error when saving heatmap: " + e.getMessage());
//        }
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
        }
    }


    public static void run() {
//        generateSequenceDatabase();
        sequenceMining();
    }

    public static class Heatmap {
        private Map<String, List<Integer>> hostData = new LinkedHashMap<>();
        private final int hostWidth = 20;
        private final int imageHeight = 500;
        private final int maxValue = 30; // max possible value you expect (can be parameterized)


        // Method to add data points
        public void addPoint(String hostIp, int value) {
            hostData.computeIfAbsent(hostIp, k -> new ArrayList<>()).add(value);
        }

        // Method to save the heatmap as an image
        public void saveImage(String filename) throws IOException {
            int imageWidth = hostData.size() * hostWidth;  // Total width of the image
            BufferedImage image = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = image.createGraphics();

            // Set white background
            g2d.setColor(Color.WHITE);
            g2d.fillRect(0, 0, imageWidth, imageHeight);

            // Draw axis (Y-axis and X-axis)
            drawAxes(g2d);

            // Draw bars for each host
            int hostIndex = 0;
            for (Map.Entry<String, List<Integer>> entry : hostData.entrySet()) {
                int baseX = hostIndex * hostWidth;
                int totalValue = entry.getValue().stream().mapToInt(Integer::intValue).sum();
                int barHeight = scaleValue(totalValue);  // Scale the total value to fit in the height

                // Draw bar for the host
                g2d.setColor(getColor(totalValue)); // Use color based on the total value
                g2d.fillRect(baseX, imageHeight - barHeight, hostWidth - 2, barHeight); // Draw the bar

                hostIndex++;
            }

            g2d.dispose();
            ImageIO.write(image, "png", new File(filename)); // Save image to file
        }

        // Method to scale the value for Y-axis (so the higher the value, the higher the bar is drawn)
        private int scaleValue(int value) {
            return (value * imageHeight) / maxValue;
        }

        // Method to get a color based on value
        private Color getColor(int value) {
            // Color gradient (Green for low, Red for high)
            int red = Math.min(255, (value * 8));
            int green = 255 - Math.min(255, (value * 8));
            return new Color(red, green, 0);
        }

        // Method to draw axis (for y-axis and x-axis)
        private void drawAxes(Graphics2D g2d) {
            // Draw Y-axis values (for each scale of the y-axis)
            g2d.setColor(Color.BLACK);
            g2d.setFont(new Font("Arial", Font.PLAIN, 12));

            for (int i = 0; i <= maxValue; i += 5) {
                int yPos = imageHeight - scaleValue(i);  // Calculate y position based on value
                g2d.drawLine(0, yPos, 10, yPos);  // Draw tick line at that position
                g2d.drawString(String.valueOf(i), 15, yPos);  // Draw value next to it
            }

            // Draw X-axis values (host IP labels)
//            int hostIndex = 0;
//            for (String host : hostData.keySet()) {
//                int xPos = hostIndex * hostWidth + hostWidth / 2;  // Middle of the host's segment
//                g2d.drawString(host, xPos - 10, imageHeight - 10);  // Label under each host's column
//                hostIndex++;
//            }
        }
    }
}

class SequenceMiningProcessor implements Callable<Void> {
    private File sequenceToMineFile;

    public SequenceMiningProcessor(File sequenceToMineFile) {
        this.sequenceToMineFile = sequenceToMineFile;
    }

    public Void call() throws Exception {
        try {
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
                algo.setMaximumPatternLength(10);
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
                    String host = sequenceToMineFile.getParentFile().getName();
                    if (SequenceDatabase.legitList.contains(host)) {
                        SequenceDatabase.legitHeatmap.addPoint(host, pattern.getPrefix().split(" -1 ").length);
                    } else if (!SequenceDatabase.omitList.contains(host)) {
                        SequenceDatabase.botHeatmap.addPoint(host, pattern.getPrefix().split(" -1 ").length);
                    }
                    if (++cnt > 15) break;
                }
                writer.flush();
                writer.close();
            } catch (Exception e) {
                System.out.println("Error when mining sequences: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.out.println("Error when mining sequences: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

}

