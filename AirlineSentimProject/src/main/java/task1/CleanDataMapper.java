package task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columns = value.toString().split(",");
		if (columns.length >= 23) {
			StringBuilder newValue = new StringBuilder();
			for (int i = 0; i < 22; i++) {
				if (i > 0) {
					newValue.append(",");
				}
				newValue.append(columns[i]);
			}
			outputKey.set(columns[0]);
			outputValue.set(newValue.toString());
			context.write(outputKey, outputValue);
		}
	}
}
