package test2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ANSValidationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static int numColumns = -1;

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (numColumns == -1) {
            numColumns = value.toString().split(",").length;
        }
        if (isValid(value.toString())) {
            context.write(key, value);
        }
    }

    private boolean isValid(String line) {
        String[] parts = line.split(",");
        return parts.length == numColumns;
    }
}
