package task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

public class CountryValidationMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> countryCodeToNameMap = new TreeMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            String cacheFilePath = cacheFiles[0].toString();
            countryCodeToNameMap = new CountryCodeReader().readCountryCodes(cacheFilePath);
            // Print the contents of the TreeMap
            for (Map.Entry<String, String> entry : countryCodeToNameMap.entrySet()) {
                System.out.println("Country Code: " + entry.getKey() + ", Country Name: " + entry.getValue());
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 14 && "negative".equals(parts[14])) {
            String countryCode = parts[10];
            String countryName = countryCodeToNameMap.get(countryCode);
            if (countryName != null) {
                context.write(new Text(countryName), new Text("1"));
            }
        }
    }
}