package task2;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReasonsReducer extends Reducer<Text, Text, Text, Text> {
    private TreeMap<Integer, String> countMap;

    @Override
    protected void setup(Context context) {
        countMap = new TreeMap<>(Collections.reverseOrder());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        countMap.clear(); // Clear the map for each key (airline)
        
        // Populate the countMap with reasons and their counts
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            String reason = parts[0];
            int count = Integer.parseInt(parts[1]);
            countMap.put(count, reason);
        }

        // Output the top 5 reasons for the current airline
        int counter = 0;
        for (Map.Entry<Integer, String> entry : countMap.entrySet()) {
            if (counter < 5) {
                context.write(key, new Text(entry.getValue() + "\t" + entry.getKey()));
                counter++;
            } else {
                break;
            }
        }
    }
}

