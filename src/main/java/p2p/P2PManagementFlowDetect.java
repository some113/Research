package p2p;

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

import java.io.*;
import java.util.*;

public class P2PManagementFlowDetect {
    static String sequenceFolder = System.getProperty("user.dir") + "/OutputData/Sequences/";

    public static class behaviourMiningMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("Value in mapper: " + value);
            String[] parts = value.toString().split(" ");
            String dstAdd = parts[0], flow = parts[1];
            context.write(new Text(dstAdd), new Text(flow));
        }
    }

    //Reducer class for P2P host detection Map-Reduce Module
    public static class behaviourMiningReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String eventFile = System.getProperty("user.dir") + "/OutputData/Sequences/" + key.toString() + "/event.txt";

            try {
                BufferedReader br = new BufferedReader(new FileReader(eventFile));
                String line;
                while ((line = br.readLine()) != null) {
                    context.write(new Text(key.toString()), new Text(line));
                }
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            BufferedWriter allWriter = null;

        }
    }


    public static void frequentBehaviourMining() {
        for (File folder : new File(sequenceFolder).listFiles()) {
//            System.out.println("Mining sequences from: " + folder.getAbsolutePath());
            try {
                BufferedReader br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/event.txt"));
                String line;
                while ((line = br.readLine()) != null) {
//                    System.out.println(line);
                    String[] parts = line.split("#");
                    String regex = parts[0].replace("-1", "*");
                    System.out.println(regex);
                }
                br.close();
            } catch (Exception e) {
                System.out.println("Error when mining behaviour: " + e.getMessage());
                e.printStackTrace();
            }
            HashMap<String, ArrayList<String>> eventMap = new HashMap<String, ArrayList<String>>(); //<prefix, writer>

        }
    }

    public static void  run() throws IllegalArgumentException, IOException {
        frequentBehaviourMining();
    }
}
