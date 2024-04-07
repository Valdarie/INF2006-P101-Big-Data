package task6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirlineSentimentDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AirlineSentimentDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create a Hadoop Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Airline Sentiment Analysis");
        job.setJarByClass(AirlineSentimentDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(AirlineSentimentMapper.class);
        job.setReducerClass(AirlineSentimentReducer.class);

        // Set input and output format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Execute the job and wait for completion
        boolean jobCompletedSuccessfully = job.waitForCompletion(true);

        if (jobCompletedSuccessfully) {
            System.out.println("Job executed successfully.");
            System.exit(0);
        } else {
            System.out.println("Job execution failed.");
            System.exit(1);
        }
    }
}
