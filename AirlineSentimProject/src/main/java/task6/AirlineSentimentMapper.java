package task6;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineSentimentMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text airline = new Text();
	private Text sentiment = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println("Processing line: " + line);

		// Skip the header row
		if (line.startsWith("_unit_id")) {
			System.out.println("Skipping header row.");
			return;
		}

		// Split the line by comma to extract relevant fields
		String[] parts = line.split(",");

		if (parts.length >= 24) {
			String airlineName = parts[16];
			String sentimentValue = parts[14];

			System.out.println("Airline: " + airlineName + ", Sentiment: " + sentimentValue);

			airline.set(airlineName);
			sentiment.set(sentimentValue);
			context.write(airline, sentiment);
		} else {
			System.out.println("Incomplete data in line, skipping: " + line);
		}
	}
}
