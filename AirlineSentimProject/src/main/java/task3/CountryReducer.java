package task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CountryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<Text, IntWritable> countryCountMap = new HashMap<>();
    private Text mostComplainedCountry = new Text();
    private IntWritable maxComplaints = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        Text country = new Text(key);
        IntWritable count = new IntWritable(sum);
        countryCountMap.put(country, count);

        // Check if this country has the most complaints
        if (sum > maxComplaints.get()) {
            mostComplainedCountry.set(country);
            maxComplaints.set(sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Remove the most complained country from the map
        countryCountMap.remove(mostComplainedCountry);

        // Write the country with the most complaints at the top
        context.write(new Text("Most complained country: " + mostComplainedCountry.toString()), maxComplaints);

        // Sort the remaining countries alphabetically and write them
        Map<Text, IntWritable> sortedMap = new TreeMap<>(countryCountMap);
        for (Map.Entry<Text, IntWritable> entry : sortedMap.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }
}