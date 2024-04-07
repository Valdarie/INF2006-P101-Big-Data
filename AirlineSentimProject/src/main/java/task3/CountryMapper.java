/**
 * 2202795 LEO EN QI VALERIE
 */

package task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class CountryMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Map<String, String> countryCodeToNameMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        countryCodeToNameMap.put(parts[0], parts[1]);
                    }
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 14 && "negative".equals(parts[14])) {
            String countryCode = parts[10];
            String countryName = countryCodeToNameMap.get(countryCode);
            if (countryName != null) {
                context.write(new Text(countryName), new IntWritable(1));
            }
        }
    }
}
