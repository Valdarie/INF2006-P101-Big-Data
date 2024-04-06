package task4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class T4ValidationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// Convert the input Text value to a String and check if it's valid
		String newValue = isValid(value.toString());
		// If the line is valid (not "FALSE"), write it to the context for further
		// processing
		if (!newValue.equals("FALSE")) {
			context.write(key, new Text(newValue));
		}
	}

	private String isValid(String line) {
		// Skip the CSV header line starting with "_unit_id"
		if (!line.startsWith("_unit_id")) {
			// Split the line into parts, expecting exactly 22 parts (based on CSV
			// structure)
			String[] parts = line.split(",", 22);
			// If there aren't exactly 22 parts, the line is considered invalid
			if (parts.length != 22) {
				return "FALSE";
			}

			// Define a pattern to match dates in the format MM/DD/YYYY within the 22nd
			// column
			String pattern = "\\d{1,2}/\\d{1,2}/\\d{4}";
			Pattern regex = Pattern.compile(pattern);
			Matcher matcher = regex.matcher(parts[21]);
			String beforeTwtCreation = null;
			int lastIdx = -1;

			// Find the last occurrence of a date pattern in the 22nd column
			while (matcher.find()) {
				lastIdx = matcher.start();
			}

			// If a date pattern was found, process the text before this date
			if (lastIdx != -1) {
				beforeTwtCreation = parts[21].substring(0, lastIdx);
				// Remove trailing ",," if present
				if (beforeTwtCreation.endsWith(",,")) {
					beforeTwtCreation = beforeTwtCreation.substring(0, beforeTwtCreation.length() - 2);
				} else {
					// Split by the last occurrence of ",[" and take the first part
					beforeTwtCreation = beforeTwtCreation.split(",\"\\[")[0];
				}
			}
			// Combine all parts (excluding the processed 22nd column) with the modified
			// 22nd column
			String joinedString = String.join(",", Arrays.copyOfRange(parts, 0, 21));

			return joinedString + "," + beforeTwtCreation;
		} else {
			// If the line is the header or otherwise invalid, return "FALSE"
			return "FALSE";
		}
	}
}