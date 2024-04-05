package task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ANSMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Hashtable<String, String> countryCodes = new Hashtable<>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException {
	
	// We will put the ISO-3166-alpha3.tsv to Distributed Cache in the driver class
	// so we can access to it here locally by its name
	
		BufferedReader br = new BufferedReader(new FileReader("twitterdata/ISO-3166-alpha3.tsv"));
		String line = null;
		
		while (true) {
			line = br.readLine();
			if (line != null) {
				String[] parts = line.split("\t");
				countryCodes.put(parts[0], parts[1]);
			} 
			
			else {
				break; // finished reading
			}
		}
		
		br.close();
	}
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming CSV structure, where the first line is headers
        String line = value.toString();
        if (line == null || line.isEmpty() || line.contains("airline_sentiment")) {
            return; // Skip header or empty line
        }

        String[] parts = line.split(",");
        // Make sure to replace these indices with the correct ones if they are not
        String airline = parts[5]; // Adjust index for 'airline'
        String sentiment = parts[14]; // Adjust index for 'airline_sentiment'
        String reason = parts[15]; // Adjust index for 'negativereason'

        if ("negative".equals(sentiment) && reason != null && !reason.isEmpty()) {
            context.write(new Text(airline + "\t" + reason), new IntWritable(1));
        }
    }
}
