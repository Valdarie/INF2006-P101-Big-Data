package task5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// This reducer class is designed to perform sentiment analysis on tweets and compare predicted sentiment with actual sentiment.
public class T5Reducer extends Reducer<Text, Text, Text, Text> {

	// Instance of T5SentiWord for sentiment analysis
	private T5SentiWord sentiWordNet;
	// Counter for total number of tweets processed
	private int totalTweets = 0;
	// Counter for the number of tweets where predicted sentiment matches the actual
	// sentiment
	private int totalMatches = 0;
	// Logger for logging information about the processing
	final Log LOG = LogFactory.getLog(T5Reducer.class);

	// Setup method is called once at the beginning of the task and is used to
	// initialize resources.
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Initialize sentiWordNet with the path to the SentiWordNet dictionary file
		sentiWordNet = new T5SentiWord("SentiWordNetData/SentiWordNet_3.0.0.txt");
	}

	// The reduce method is called for each key and its list of values. Here, it
	// processes each tweet and its sentiment.
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Iterate over all values (sentiments) associated with the key (tweet)
		for (Text value : values) {
			// Increment the total count of tweets processed
			totalTweets++;
			// Convert the key (tweet) to a string
			String tweet = key.toString();
			// Log the processing of the current tweet
			LOG.info("Processing Tweet: " + tweet);

			// Get the actual sentiment from the value
			String actualSentiment = value.toString();
			// Log the actual sentiment of the tweet
			LOG.info("Actual Sentiment: " + actualSentiment);

			// Analyze the tweet to predict its sentiment
			T5SentiWord.SENTIMENT predictedSentimentEnum = sentiWordNet.analyze(tweet);
			// Convert the predicted sentiment to a string and to lowercase for comparison
			String predictedSentiment = predictedSentimentEnum.toString().toLowerCase();

			// Log the predicted sentiment for the tweet
			LOG.info("Predicted Sentiment: " + predictedSentiment);

			// If the predicted sentiment matches the actual sentiment, increment the total
			// matches counter
			if (predictedSentiment.equals(actualSentiment)) {
				totalMatches++;
			}
		}
	}

	// The cleanup method is called once at the end of the task and is used for
	// cleanup and to write final output.
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Calculate the accuracy of sentiment prediction matches as a value between 0
		// and 1
		double accuracy = (double) totalMatches / totalTweets;

		// Write the total number of tweets processed to the context
		context.write(new Text("Number of Tweets: "), new Text(String.valueOf(totalTweets)));
		// Write the total number of matches to the context
		context.write(new Text("How many matches: "), new Text(String.valueOf(totalMatches)));
		// Write the accuracy of sentiment prediction to the context, formatted to show
		// up to two decimal places
		context.write(new Text("Accuracy: "), new Text(String.format("%.2f", accuracy)));
	}

}
