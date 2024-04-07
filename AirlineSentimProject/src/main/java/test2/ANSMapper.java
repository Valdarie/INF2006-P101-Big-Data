package test2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ANSMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isEmpty()) {
            return; // Skip empty lines
        }

        String[] parts = line.split(",");
        if (parts.length < 3) {
            return;
        }

        String airline = parts[16];
        String sentiment = parts[14]; 
        String reason = parts[15]; 

        if ("negative".equals(sentiment) && reason != null && !reason.isEmpty()) {
            context.write(new Text(airline + "\t" + reason), one);
        }
    }
}
