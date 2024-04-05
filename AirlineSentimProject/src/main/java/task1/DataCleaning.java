package task1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCleaning {

    public static class CleanDataMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length >= 23) {
                StringBuilder newValue = new StringBuilder();
                for (int i = 0; i < 23; i++) {
                    if (i > 0) {
                        newValue.append(",");
                    }
                    newValue.append(columns[i]);
                }
                outputKey.set(columns[3]); // Assuming _id is at index 3
                outputValue.set(newValue.toString());
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class CleanDataReducer extends Reducer<Text, Text, Text, Text> {

        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueRecords = new HashSet<>();
            for (Text value : values) {
                String record = value.toString();
                if (!uniqueRecords.contains(record)) {
                    outputValue.set(record);
                    context.write(null, outputValue);
                    uniqueRecords.add(record);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Cleaning");
        job.setJarByClass(DataCleaning.class);
        job.setMapperClass(CleanDataMapper.class);
        job.setReducerClass(CleanDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
