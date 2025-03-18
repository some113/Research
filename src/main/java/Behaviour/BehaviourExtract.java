package Behaviour;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BehaviourExtract {
    /*
        Input format
        [srcAdd] [proto],[dstAdd],[bppin
     */
    public static class EventMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    public static class EventReducer extends Reducer<Object, Text, Text, Text> {
        @Override
        protected void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write((Text) key, value);
            }
        }

    }
}
