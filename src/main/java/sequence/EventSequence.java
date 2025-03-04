package sequence;

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
import p2p.P2PHostIdentify;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import Config.Config;

import static java.lang.Math.random;

public class EventSequence {

    public static int T = Config.T;

    private static String host = "";

    public static BufferedWriter writer = null;
    /**
        Input format
        + key: [srcAdd]
        + value: [time],[dstAdd],[bppout],[totalBytes]
        Output format
        + key: [time] [dstAdd]
        + value: [bppout]
     */
    public static class EventSequenceGenerateMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            int time = Integer.parseInt(parts[0]);
            String dstAdd = parts[1];
            float bppout = Float.parseFloat(parts[2]);
            int totalBytes = Integer.parseInt(parts[3]);

            context.write(new Text((time / T) + " " +dstAdd), new Text("" + bppout));
        }
    }

    public static class EventSequenceGenerateReducer extends Reducer<Object, Text, Text, Text> {
        @Override
        protected void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            File eventSequenceFile = new File(System.getProperty("user.dir") + "/OutputData/EventSequences/"
                    + host + ".txt");
            writer = new BufferedWriter(new FileWriter(eventSequenceFile, true));

            for (Text value : values) {
                writer.write(((int) Float.parseFloat(value.toString())) + " -1 ");
            }
            writer.write("-2\n");
            writer.flush();
        }

    }

    public static void run() throws IllegalArgumentException, IOException {

        File dir = new File(System.getProperty("user.dir") + "/OutputData/EventSequences/");
        if (!dir.exists()) {
            dir.mkdir();
        }

        //Create MAP-REDUCE job for detecting P2P flows.
//        BasicConfigurator.configure();
        JobConf conf = new JobConf(EventSequence.class);

        //FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION  + "/p2p_host_detection"));
        conf.setJobName("event_sequence_generate");

// Loop through directories in OutputData/Events
        for (File file : new File(System.getProperty("user.dir") + "/OutputData/Events").listFiles()) {
            if (!file.isDirectory()) continue;
            // Create Job once outside the loop
            Job jobEventSequenceGenerate = Job.getInstance(conf, "Job_event_sequence_generate_");
            jobEventSequenceGenerate.setJarByClass(EventSequence.class);
            jobEventSequenceGenerate.setMapperClass(EventSequence.EventSequenceGenerateMapper.class);
            jobEventSequenceGenerate.setReducerClass(EventSequence.EventSequenceGenerateReducer.class);
            jobEventSequenceGenerate.setOutputKeyClass(Text.class);
            jobEventSequenceGenerate.setOutputValueClass(Text.class);
            jobEventSequenceGenerate.setNumReduceTasks(5);  // Set reduce tasks before job submission

// Set output path for the job
            FileOutputFormat.setOutputPath(jobEventSequenceGenerate,
                    new Path(System.getProperty("user.dir") + "/OutputData/EventSequenceTemp" + random()));

// Initialize JobControl once
            JobControl jobCtrl = new JobControl("ctrl_event_sequence_generate");

            // Add input path for each subdirectory
            FileInputFormat.addInputPath(jobEventSequenceGenerate, new Path(file.getPath()));

            // Set the host variable, used inside the loop
            host = file.getName();

            // Create a controlled job and add to job control (only add after configuring the job)
            ControlledJob ctrlEventSequenceGenerate = new ControlledJob(conf);
            ctrlEventSequenceGenerate.setJob(jobEventSequenceGenerate);
            jobCtrl.addJob(ctrlEventSequenceGenerate);  // Add the job only after full configuration

            // Start the JobControl in a separate thread
            Thread t = new Thread(jobCtrl);
            t.start();

            // Wait for all jobs to finish (check job control status)
            while (!jobCtrl.allFinished()) {
                try {
                    Thread.sleep(1000);  // Sleep to avoid busy-waiting
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // Handle thread interruption
                }
            }

            // Output successful jobs
            System.out.println(jobCtrl.getSuccessfulJobList());

            // Stop the job control after all jobs have finished
            jobCtrl.stop();

        }

// Close the writer outside the loop once all jobs are done
        writer.flush();
        writer.close();



    }

}
