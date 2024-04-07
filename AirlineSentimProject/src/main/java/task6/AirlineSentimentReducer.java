package task6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirlineSentimentReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println("Reducer: Processing key: " + key.toString());

		int positiveCount = 0;
		int negativeCount = 0;
		int neutralCount = 0;

		// Iterate through all values for the given key (airline)
		for (Text value : values) {
			String sentiment = value.toString();

			if (sentiment.equals("positive")) {
				positiveCount++;
			} else if (sentiment.equals("negative")) {
				negativeCount++;
			} else if (sentiment.equals("neutral")) {
				neutralCount++;
			}
		}

		// Calculate overall sentiment score
		int sentimentScore = positiveCount - negativeCount;

		// Output the results
		context.write(key, new Text("Positive Sentiment: " + positiveCount + "\tNegative Sentiment: " + negativeCount
				+ "\tNeutral Sentiment: " + neutralCount + "\tOverall Sentiment: " + sentimentScore));
	}
}
