/**
 * 2202217 Teo Xuanting
 */

package task1;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString();
        
        // Skip header
        if (!line.startsWith("_unit_id")) {
            // Split the line by comma
            String[] parts = line.split(",", -1);
            
            // Check if the parts length is at least 24 (to avoid index out of bounds)
            if (parts.length >= 24) {
                // Concatenate the first 23 columns
                StringBuilder newLineBuilder = new StringBuilder();
                for (int i = 0; i < 23; i++) {
                    newLineBuilder.append(parts[i]);
                    if (i < 22) {
                        newLineBuilder.append(",");
                    }
                }
                String newLine = newLineBuilder.toString();
                
                // Emit _id as key and the line (excluding columns from index 23 onwards) as value
                context.write(new Text(parts[3]), new Text(newLine));
            }
        }
    }
}
