
//Done by Bryon Tian

package task4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class T4Reducer extends Reducer<Text, FloatWritable, Text, Text> {
    // TreeMap to keep the channels and their list of trust points sorted by channel name
    private TreeMap<Text, List<Float>> treeMap = new TreeMap<>();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        // Prepare a list to collect trust points for the current channel (key)
        List<Float> trustPointsList = new ArrayList<>();
        
        // Loop through each trust point and add it to the list
        for (FloatWritable value : values) {
            trustPointsList.add(value.get()); // Convert FloatWritable to Float and add to list
        }

        // Put the channel and its list of trust points into the TreeMap
        // A new Text object is created for the key to avoid reference issues
        treeMap.put(new Text(key), trustPointsList);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Iterate over the TreeMap entries
        for (Map.Entry<Text, List<Float>> entry : treeMap.entrySet()) {
            // Extract the channel (key) and its list of trust points (value)
            Text channel = entry.getKey();
            List<Float> trustPoints = entry.getValue();
            
            // Sort the list of trust points to prepare for median calculation
            Collections.sort(trustPoints);

            // Calculate the mean of the trust points using the stream API
            float sum = trustPoints.stream().reduce(0f, Float::sum); 
            float mean = sum / trustPoints.size();

            // Calculate the median of the trust points
            float median;
            int size = trustPoints.size();
            if (size % 2 == 0) {
                // For even size, median is the average of the two middle numbers
                median = (trustPoints.get(size / 2 - 1) + trustPoints.get(size / 2)) / 2.0f;
            } else {
                // For odd size, median is the middle number
                median = trustPoints.get(size / 2);
            }

            // Format the mean and median into a string for output
            String combinedValue = String.format("Mean: %.2f, Median: %.2f", mean, median);
            // Write the channel (key) and the formatted mean and median (value) to context
            context.write(channel, new Text(combinedValue));
        }
    }
}
