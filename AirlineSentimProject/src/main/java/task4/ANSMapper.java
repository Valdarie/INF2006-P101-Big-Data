package task4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class ANSMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
 Text channel = new Text();
 FloatWritable trustPoint = new FloatWritable();

 // Map function processes each input and extracts relevant information (Part 8 and Part 16)
 // The output is a key-value pair with the airline provider as the key and trust point as the value
 @Override
 public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
     String[] parts = value.toString().split(",");
     // Retrieving the trust point and channel from the csv and trimming the data when needed
     Float trustPointValue = 0.0f;

     if (parts.length >= 17) {

         trustPointValue = Float.parseFloat(parts[8]);
         String channelString = parts[7].trim();
         if (!channelString.isEmpty()) {
             channel.set(channelString);
             trustPoint.set(trustPointValue);

             // Output the key-value pair with airline provider as the key and trust point as the value
             context.write(channel, trustPoint);
         }
     }
 }
}
