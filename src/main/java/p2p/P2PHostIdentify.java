package p2p;

import java.io.*;
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
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec;

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

    private static int PACKET_CLUSTER_NUMBER = Config.PACKET_CLUSTER_NUMBER;


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
            if (srcAdd.equals("172.16.2.113") || srcAdd.equals("151.46.205.64") || srcAdd.equals("177.69.26.73") || srcAdd.equals("204.156.230.173") || srcAdd .equals("205.171.140.156")) {
                return;
            }
            context.write(new Text(srcAdd), new Text("" + time + " " + dstAdd + " " + flow));
        }
    }

    public static class FlowWithTime {
        public String flow;
        public int time;
        public FlowWithTime(String flow, int time) {
            this.flow = flow;
            this.time = time;
        }
    }

    //Reducer class for P2P host detection Map-Reduce Module
    public static class P2PHostDetectionReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
//            System.out.println("Key value: " + key);
            Set<String> dstSet = new HashSet<String>();
            HashMap<Integer, HashMap<String, ArrayList<FlowWithTime>>> flowMap = new HashMap<Integer, HashMap<String,ArrayList<FlowWithTime>>>(); //<Sequence, <DstAdd, <Flows, Time>> >

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

            List<Text> valueList = new ArrayList<Text>();
            for (Text val : values) {
                valueList.add(new Text(val));
            }
//            values.forEach(valueList::add);
            valueList.sort(new Comparator<Text>() {
                @Override
                public int compare(Text o1, Text o2) {
                    int endtime1 = Integer.parseInt(o1.toString().substring(0, o1.toString().indexOf(" ")).substring(0, o1.toString().indexOf(".")));
                    int endtime2 = Integer.parseInt(o2.toString().substring(0, o2.toString().indexOf(" ")).substring(0, o2.toString().indexOf(".")));
                    return endtime1 - endtime2;
                }
            });

            for (Text val : valueList) {

                String[] parts = val.toString().split(" ");
                int endTime = Integer.parseInt(parts[0].substring(0, parts[0].indexOf(".")));
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

               printFlowByCluster(flow, writer);
//                writer.write(flow.replace(",", " -1 ") + " -1 -2\n");
//                writer.flush();

                if (!flowMap.containsKey(i)) {
                    flowMap.put(i, new HashMap<String, ArrayList<FlowWithTime>>());
                }
                if (!flowMap.get(i).containsKey(dstAdd)) {
                     flowMap.get(i).put(dstAdd, new ArrayList<FlowWithTime>());
                }

                flowMap.get(i).get(dstAdd).add(new FlowWithTime(flow, endTime));

//                if (!sequenceToMineWriterMap.containsKey("all" + srcAdd)) {
//                    File file = new File(System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/all.txt");
//                    sequenceToMineWriterMap.put("all" + srcAdd, new BufferedWriter(new FileWriter(
//                            System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/all.txt", true)));
//                }

//                allWriter = sequenceToMineWriterMap.get("all" + srcAdd);
//
//                allWriter.write(dstAdd + " " + flow + "\n");
//                allWriter.flush();

                context.write(new Text(srcAdd), new Text(endTime + " " + dstAdd + " " + flow));
            }

            if (flowMap.size() == 0) {
                dir = new File(System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/");
                dir.delete();
                dir = new File(System.getProperty("user.dir") + "/OutputData/SequencesToMine/" + srcAdd + "/");
                dir.delete();
                return;
            }

            for (Integer sequenceNumber : flowMap.keySet()) {
                File sequenceFile = new File(System.getProperty("user.dir") + "/OutputData/Sequences/" + srcAdd + "/seq" + sequenceNumber + ".txt");
                allWriter = new BufferedWriter(new FileWriter(sequenceFile, true));
                for (String dstAdd : flowMap.get(sequenceNumber).keySet()) {
                    allWriter.write(dstAdd + " ");

                    String timeString = "";

                    for (int i = 0; i < flowMap.get(sequenceNumber).get(dstAdd).size(); i++) {
                        FlowWithTime flow = flowMap.get(sequenceNumber).get(dstAdd).get(i);
//                        timeString += (int)flow.time + " ";
                        allWriter.write(flow.flow + " " + (int) flow.time);
                        if (i < flowMap.get(sequenceNumber).get(dstAdd).size() - 1) {
                            allWriter.write(".");
                        }
                    }
//                    allWriter.write(" #" + timeString);
                    allWriter.write("\n");
                }
                allWriter.write("\n");

                allWriter.flush();
            }
                allWriter.close();
            if (writer != null) writer.flush();
            //writer.close();
        }
    }

    static void printFlowByCluster(String flow, Writer writer) throws IOException {

        String[] parts = flow.split(",");
        HashSet<String> clusterSet = new HashSet<String>();
        for (int i = 0;i < parts.length; i++) {
            clusterSet.add(parts[i]);
            if (i % PACKET_CLUSTER_NUMBER == PACKET_CLUSTER_NUMBER - 1) {
                for (String packet : clusterSet) {
                    writer.write(packet + " ");
                }
                writer.write("-1 ");
                clusterSet.clear();
            }
        }
        if (parts.length % PACKET_CLUSTER_NUMBER != 0) {
            for (String packet : clusterSet) {
                writer.write(packet + " ");
            }
            writer.write("-1 ");
        }
        writer.write("-2\n");
        writer.flush();
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
