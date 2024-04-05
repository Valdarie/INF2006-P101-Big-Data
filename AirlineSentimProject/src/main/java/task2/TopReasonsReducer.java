package task2;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReasonsReducer extends Reducer<Text, IntWritable, Text, Text> {
    private TreeMap<Integer, String> countMap;

    @Override
    protected void setup(Context context) {
        countMap = new TreeMap<>(Collections.reverseOrder());
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        countMap.put(sum, key.toString());

        // Keep only top 5 reasons
        if (countMap.size() > 5) {
            countMap.remove(countMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> entry : countMap.entrySet()) {
            context.write(new Text(entry.getValue()), new Text(entry.getKey().toString()));
        }
    }
}
