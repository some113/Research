package p2p;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Inet4Address;
import java.util.*;

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

public class P2PHostIdentify {
    //P2P host detection threshold for detecting P2P flows
    private static int p2PHostDetectionThreshold = Config.BGPSupport;
    //Byte per packet threshold for merging flows
    private static int bytePerPacketThreshold = Config.NumberOfByteThread;

    private static int NumberOfPacketThread = Config.NumberOfPacketThread;

    private static String P2PFlowFileName = Config.P2PFlowFileName;

    static HashMap<String, BufferedWriter> sequenceToMineWriterMap = new HashMap<String, BufferedWriter>(); //<prefix, writer>

    static HashMap<String, BufferedWriter> sequenceWriterMap = new HashMap<String, BufferedWriter>(); //<prefix, writer>

    private static int T = Config.T;

    private static BufferedWriter writer = null;


    //Mapper class for P2P host detection Map-Reduce Module
    public static class P2PHostDetectionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("Value in mapper: " + value);
            String[] parts = value.toString().split(" ");
            String time = parts[0], srcAdd = parts[2], dstAdd = parts[3], flow = parts[4];
            // TODO: Remove and process IP6 too
            if (srcAdd.contains(":") || dstAdd.contains(":")) {
                return;
            }
            srcAdd = srcAdd.substring(0, srcAdd.lastIndexOf("."));
            dstAdd = dstAdd.substring(0, dstAdd.lastIndexOf("."));
            context.write(new Text(srcAdd), new Text("" + time + " " + dstAdd + " " + flow));
        }
    }

    //Reducer class for P2P host detection Map-Reduce Module
    public static class P2PHostDetectionReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
//            System.out.println("Key value: " + key);
            Set<String> dstSet = new HashSet<String>();
            HashMap<Integer, HashMap<String, ArrayList<String>>> flowMap = new HashMap<Integer, HashMap<String,ArrayList<String>>>(); //<prefix, flow>>

            String srcAdd = key.toString();
            File dir = new File(System.getProperty("user.dir") + "/OutputData/Sequences/");
            if (!dir.exists()) {
                dir.mkdir();
            }
            dir = new File(System.getProperty("user.dir") + "/OutputData/SequencesToMine/");
            if (!dir.exists()) {
                dir.mkdir();
            }

            for (Text val : values) {
                String[] parts = val.toString().split(" ")[1].split("\\.");
                String prefix = parts[0] + "." + parts[1];

                if (!dstSet.contains(prefix)) {
                    dstSet.add(prefix);
                }
                if (dstSet.size() >= p2PHostDetectionThreshold) {
                    break;
                }
            }

            if (dstSet.size() < p2PHostDetectionThreshold) {
                return;
            }
//    }

            BufferedWriter allWriter = null;
            dir = new File(System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/");

            dir.mkdir();

            dir = new File(System.getProperty("user.dir") + "/OutputData/SequencesToMine/" + srcAdd + "/");

            dir.mkdir();

            for (Text val : values) {

                String[] parts = val.toString().split(" ");
                float endTime = Float.parseFloat(parts[0]);
                String dstAdd = parts[1], flow = parts[2], sizes[] = flow.split(",");
                if (sizes.length < NumberOfPacketThread) {
                    continue;
                }
                int sum = 0;
                for (String size : flow.split(",")) {
                    sum += Integer.parseInt(size);
                }
                if ((float)sum / sizes.length > bytePerPacketThreshold) {
                    continue;
                }

                int i = (int) endTime / T;

                if (!sequenceToMineWriterMap.containsKey(srcAdd + i)) {
                    File file = new File(System.getProperty("user.dir") + "/OutputData/SequencesToMine/" + srcAdd + "/seq" + i + ".txt");
                    sequenceToMineWriterMap.put(srcAdd + i, new BufferedWriter(new FileWriter(
                            System.getProperty("user.dir") + "/OutputData/SequencesToMine/" + srcAdd + "/seq" + i + ".txt", true)));
                }
                writer = sequenceToMineWriterMap.get(srcAdd + i);

                writer.write(flow.replace(",", " -1 ") + " -1 -2\n");
                writer.flush();

                if (!flowMap.containsKey(i)) {
                    flowMap.put(i, new HashMap<String, ArrayList<String>>());
                }
                if (!flowMap.get(i).containsKey(dstAdd)) {
                     flowMap.get(i).put(dstAdd, new ArrayList<String>());
                }
                flowMap.get(i).get(dstAdd).add(flow);

//                if (!sequenceToMineWriterMap.containsKey("all" + srcAdd)) {
//                    File file = new File(System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/all.txt");
//                    sequenceToMineWriterMap.put("all" + srcAdd, new BufferedWriter(new FileWriter(
//                            System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/all.txt", true)));
//                }

//                allWriter = sequenceToMineWriterMap.get("all" + srcAdd);
//
//                allWriter.write(dstAdd + " " + flow + "\n");
//                allWriter.flush();

                context.write(new Text(srcAdd), new Text(dstAdd + " " + flow));
            }

            for (Integer sequenceNumber : flowMap.keySet()) {
                File sequenceFile = new File(System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/seq" + sequenceNumber + ".txt");
                allWriter = new BufferedWriter(new FileWriter(sequenceFile, true));
                for (String dstAdd : flowMap.get(sequenceNumber).keySet()) {
                    allWriter.write(dstAdd + " ");
                    for (String flow : flowMap.get(sequenceNumber).get(dstAdd)) {
                        allWriter.write(flow);
                        if (!flow.equals(flowMap.get(sequenceNumber).get(dstAdd).get(flowMap.get(sequenceNumber).get(dstAdd).size() - 1))) {
                            allWriter.write(".");
                        }
                    }
                }
                allWriter.write("\n");

                allWriter.flush();
            }
            allWriter.close();
            if (writer != null) writer.flush();
            //writer.close();
        }
    }



    public static void run() throws IllegalArgumentException, IOException {

        //Create MAP-REDUCE job for detecting P2P flows.
//        BasicConfigurator.configure();
        JobConf conf = new JobConf(P2PHostIdentify.class);

        //FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION  + "/p2p_host_detection"));
        org.apache.commons.io.FileUtils.deleteDirectory(new File(System.getProperty("user.dir") +"/OutputData"));
        conf.setJobName("p2p_host_detection");

        Job jobP2PHostDetection = Job.getInstance();
        jobP2PHostDetection.setJobName("Job_p2p_host_detection_");
        jobP2PHostDetection.setJarByClass(P2PHostIdentify.class);
        jobP2PHostDetection.setMapperClass(P2PHostDetectionMapper.class);
        jobP2PHostDetection.setReducerClass(P2PHostDetectionReducer.class);
        jobP2PHostDetection.setOutputKeyClass(Text.class);
        jobP2PHostDetection.setOutputValueClass(Text.class);
        ControlledJob ctrlJobP2PHostDetection = new ControlledJob(conf);
        ctrlJobP2PHostDetection.setJob(jobP2PHostDetection);

        FileInputFormat.addInputPath(jobP2PHostDetection,
                new Path(System.getProperty("user.dir") +"/InputData/Flow"));
//        FileInputFormat.addInputPath(jobP2PHostDetection,
//                new Path(PeerCatcherConfigure.ROOT_LOCATION + "/INPUT/P2P"));
        FileOutputFormat.setOutputPath(jobP2PHostDetection,
                new Path(System.getProperty("user.dir") +"/OutputData/P2P_detect"));
        // set the number of tasks for the reduce part of the job
        jobP2PHostDetection.setNumReduceTasks(18);

        // Run job
        JobControl jobCtrl = new JobControl("ctrl_p2p_host_detection");
        jobCtrl.addJob(ctrlJobP2PHostDetection);
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                writer.flush();
                writer.close();
                break;
            }
        }

        for (Map.Entry<String, BufferedWriter> entry : sequenceToMineWriterMap.entrySet()) {
            entry.getValue().close();
        }
    }

}
