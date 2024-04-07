package test2;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ANSReducer extends Reducer<Text, IntWritable, Text, Text> {
    private TreeMap<String, TreeMap<Integer, String>> countMap;

    @Override
    protected void setup(Context context) {
        countMap = new TreeMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String airline = key.toString().split("\t")[0];
        String reason = key.toString().split("\t")[1];
        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }

        countMap.putIfAbsent(airline, new TreeMap<>(Collections.reverseOrder()));
        countMap.get(airline).put(sum, reason);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, TreeMap<Integer, String>> entry : countMap.entrySet()) {
            String airline = entry.getKey();
            TreeMap<Integer, String> reasonsMap = entry.getValue();

            // Keep only the top 5 reasons
            while (reasonsMap.size() > 5) {
                reasonsMap.pollLastEntry();
            }

            for (Map.Entry<Integer, String> reasonEntry : reasonsMap.entrySet()) {
                context.write(new Text(airline), new Text(reasonEntry.getValue() + "\t" + reasonEntry.getKey()));
            }
        }
    }
}
