package task6;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineSentimentMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text airline = new Text();
	private Text sentimentAndReason = new Text();

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
			String tweetText = parts[21];
			String airlineName = parts[16];
			String sentiment = parts[14];
			String reason = (parts.length > 15) ? parts[15] : "Unknown"; // In case reason field is missing

			// Include a placeholder for reason if sentiment is "neutral" or "positive"
			if (sentiment.equals("neutral") || sentiment.equals("positive")) {
				reason = "N/A";
			}

			System.out.println("Airline: " + airlineName + ", Sentiment: " + sentiment + ", Reason: " + reason);

			airline.set(airlineName);
			sentimentAndReason.set(sentiment + "\t" + reason);
			context.write(airline, sentimentAndReason);
		} else {
			System.out.println("Incomplete data in line, skipping: " + line);
		}
	}

}
