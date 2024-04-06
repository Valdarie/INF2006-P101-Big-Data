/**
 * 2202217 Teo Xuanting
 */

package task1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanDataReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> uniqueIds = new HashSet<>();
		for (Text value : values) {
			String[] columns = value.toString().split(",");
			String id = columns[3]; // Assuming _id is at index 3
			if (!uniqueIds.contains(id)) {
				outputValue.set(value);
				context.write(null, outputValue);
				uniqueIds.add(id);
			}
		}
	}
}
