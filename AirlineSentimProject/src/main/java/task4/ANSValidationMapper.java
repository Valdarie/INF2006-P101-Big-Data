package task4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ANSValidationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String newValue = isValid(value.toString());
		if (!newValue.equals("FALSE")) {
			context.write(key, new Text(newValue));
		}
	}

	private String isValid(String line) {
		// Skip header
		if (!line.startsWith("_unit_id")) {
			String[] parts = line.split(",", 22);
			if (parts.length != 22) {
				return "FALSE";
			}
			
	        String pattern = "\\d{1,2}/\\d{1,2}/\\d{4}";
	        Pattern regex = Pattern.compile(pattern);
	        Matcher matcher = regex.matcher(parts[21]);
	        String beforeTwtCreation = null;
	        int lastIdx = -1;

	        while (matcher.find()) {
	        	lastIdx = matcher.start();
	        }

	        if (lastIdx != -1) {
	        	// Remove remaining data
	        	beforeTwtCreation = parts[21].substring(0, lastIdx);
	            if (beforeTwtCreation.endsWith(",,")) {
	            	beforeTwtCreation = beforeTwtCreation.substring(0,
	            			beforeTwtCreation.length() - 2);
	            } else {
	            	beforeTwtCreation = beforeTwtCreation.split(",\"\\[")[0];
	            }
	        }
	        String joinedString = String.join(",", Arrays.copyOfRange(parts, 0, 21));

	        return joinedString + "," + beforeTwtCreation;
		} else {
			return "FALSE";
		}
	}
}