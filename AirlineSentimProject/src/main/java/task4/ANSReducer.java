package task4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.thirdparty.com.google.common.collect.TreeMultimap;
import java.io.IOException;
import java.util.*;


public class ANSReducer extends Reducer<Text, FloatWritable, Text, Text> {
	Text channel;
	FloatWritable trustPoint;
	Text trustPointMeanMedian;

	TreeMultimap<Text, FloatWritable> treeMultimap = TreeMultimap.create();
	TreeMap<Text, List<FloatWritable>> treeMap = new TreeMap<>();

	// This function uses an additional library which is the Guava class
	// TreeMultiMap to automatically stores multiple trust points for each airline
	// provider.
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		channel = new Text(key);
		List<FloatWritable> listOfFloatValues = new ArrayList<>();

		for (FloatWritable value : values) {
			trustPoint = new FloatWritable(value.get());
			listOfFloatValues.add(trustPoint);
		}

		treeMap.put(channel, listOfFloatValues);
	}

	// This function is used after the treeMultiMap had stored multiple trust point.
	// This function iterates over treeMultiMap keys and sorts them in ascending
	// order.
	// The function checks whether the number of trust point is even or odd, which
	// changes the calculation of the median
	// Cleanup is used to process the data after the reduce function.
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Iterate over TreeMap keys
		for (Text key : treeMap.keySet()) {
			List<FloatWritable> trustPoints = treeMap.get(key);
			Collections.sort(trustPoints);

			int size = trustPoints.size();
			float sum = 0;
			float median;

			// Calculate the sum of all trust points for the mean
			for (FloatWritable val : trustPoints) {
				sum += val.get();
			}
			float mean = sum / size;

			// Calculate the median
			if (size % 2 == 0) {
				median = (trustPoints.get(size / 2 - 1).get() + trustPoints.get(size / 2).get()) / 2.0f;
			} else {
				median = trustPoints.get(size / 2).get();
			}

			// Combine mean and median into a single Text value
			String combinedValue = String.format("%.2f, %.2f", mean, median);

			
			channel = new Text(key);
			trustPointMeanMedian = new Text(combinedValue);
			
			
			// Emit the channel and the combined mean and median
			context.write(channel, trustPointMeanMedian);
		}
	}
}
