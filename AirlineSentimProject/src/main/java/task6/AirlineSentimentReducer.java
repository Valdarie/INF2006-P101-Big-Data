package task6;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirlineSentimentReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println("Reducer: Processing key: " + key.toString());

		int positiveCount = 0;
		int negativeCount = 0;
		Map<String, Integer> reasonCount = new HashMap<>();

		// Iterate through all values for the given key (airline)
		for (Text value : values) {
			String[] parts = value.toString().split("\t");
			if (parts.length >= 2) {
				String sentiment = parts[0];
				String reason = parts[1];

				if (sentiment.equals("positive")) {
					positiveCount++;
				} else if (sentiment.equals("negative")) {
					negativeCount++;
				}

				// Count reasons
				if (reasonCount.containsKey(reason)) {
					reasonCount.put(reason, reasonCount.get(reason) + 1);
				} else {
					reasonCount.put(reason, 1);
				}
			} else {
				// Print a warning message for unexpected input format
				System.err.println("Reducer: Unexpected input format: " + value.toString());
			}
		}

		// Output the results
		context.write(key, new Text("Positive Sentiment: " + positiveCount + "\tNegative Sentiment: " + negativeCount
				+ "\tTop Reasons: " + getTopReasons(reasonCount)));
	}

	// Function to get the top reasons sorted in descending order with "N/A" reason at the end
	private String getTopReasons(Map<String, Integer> reasonCount) {
		// Sort the reasons by count in descending order
		Map<String, Integer> sortedReasons = reasonCount.entrySet().stream()
				.sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).collect(Collectors.toMap(
						Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));

		// Move "N/A" reason to the end if present
		if (sortedReasons.containsKey("N/A")) {
			int naCount = sortedReasons.remove("N/A");
			sortedReasons.put("N/A", naCount);
		}

		StringBuilder topReasons = new StringBuilder();
		int count = 0; // Initialize count variable

		// Append top reasons to the StringBuilder
		for (Map.Entry<String, Integer> entry : sortedReasons.entrySet()) {
			if (count < 5) {
				topReasons.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
				count++;
			} else {
				break;
			}
		}

		return topReasons.toString();
	}

}