
//Done by Osama Rasheed Khan

package task5;

// Importing necessary classes from the Hadoop library
import java.io.IOException;
import org.apache.hadoop.io.LongWritable; // Hadoop's serialization type for a long
import org.apache.hadoop.io.Text; // Hadoop's serialization type for string
import org.apache.hadoop.mapreduce.Mapper; // Base class for MapReduce's Mapper

// This class is designed to preprocess the input text, specifically filtering and transforming data for sentiment analysis.
public class T5SentiValMapper extends Mapper<LongWritable, Text, Text, Text> {
    // The map method is overridden to specify the custom mapping process.
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the input text value to a standard Java String for processing
        String line = value.toString();

        // Check if the line is a header or contains specific keywords, and skip processing if it does
        if (line.contains("_unit_id") || line.contains("airline_sentiment")) {
            return; // Skipping the header or irrelevant lines
        }
        
        // Split the line into parts using a regex that considers CSV formatting quirks (handling commas inside quotes)
        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        // Try-catch block to handle any exceptions that might occur during parsing and processing of the line
        try {
            // Check if the tweet text and sentiment fields are not empty before processing
            if (!parts[21].isEmpty() && !parts[17].isEmpty()) {
                // Clean up the tweet text by removing leading and trailing quotes
                String tweet = parts[21].replaceAll("^\"+|\"+$", "");
                // Write the cleaned tweet text and its corresponding sentiment to the context as key-value pairs
                context.write(new Text(tweet), new Text(parts[17]));
            }
        } catch (Exception e) {
            // Exception handling block, currently does nothing but can be used to log errors or take corrective action
        }
    }
    // Additional comments and methods can be added here if needed for clarity or functionality.
}
