package task5;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DriverClass {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// Create a Configuration object to hold Hadoop configuration settings
		Configuration hadoopConfiguration = new Configuration();
		// Separate configuration for the validation mapper to potentially customize
		// settings
		Configuration validationMapConf = new Configuration(false);

		// Parse the command-line arguments passed to the application
		String[] otherArgs = new GenericOptionsParser(hadoopConfiguration, args).getRemainingArgs();
		// Check for correct number of arguments, expecting input and output paths at
		// minimum
		if (otherArgs.length < 2) {
			System.err.println("Error: Incorrect number of arguments. Expected at least 2 arguments.");
			System.exit(2);
		}

		// Extract input and output paths from command-line arguments
		Path inPath = new Path(otherArgs[0]);
		Path outPath = new Path(otherArgs[1]);

		// Delete the output path if it already exists to avoid job failure
		FileSystem fs = outPath.getFileSystem(hadoopConfiguration);
		if (fs.delete(outPath, true)) {
			System.out.println("Notice: Output path exists and was deleted successfully.");
		} else {
			System.out.println("Notice: Output path does not exist or could not be deleted.");
		}

		// Configure the job for "Airline Negative Sentiments" analysis
		Job job = Job.getInstance(hadoopConfiguration, "Airline Negative Sentiments");
		// Specify the JAR file that contains the driver class
		job.setJarByClass(DriverClass.class);

		// Add the mapper to the job configuration
		System.out.println("Configuring mapper...");
		ChainMapper.addMapper(job, T5SentiValMapper.class, LongWritable.class, Text.class, Text.class, Text.class,
				validationMapConf);
		System.out.println("Success: Mapper added.");

		// Set the reducer class for the job
		job.setReducerClass(T5Reducer.class);
		System.out.println("Success: Reducer class set.");

		// Add input path with specific input format
		MultipleInputs.addInputPath(job, inPath, TextInputFormat.class, ChainMapper.class);
		System.out.println("Success: Input path and format configured.");

		// Set the output key and value classes for the job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Set the output path for the job
		FileOutputFormat.setOutputPath(job, outPath);

		// Submit the job to the cluster and wait for it to complete
		System.out.println("Submitting job...");
		boolean success = job.waitForCompletion(true);
		// Exit with appropriate status based on job success or failure
		if (success) {
			System.out.println("Job completed successfully.");
			System.exit(0);
		} else {
			System.out.println("Job did not complete successfully.");
			System.exit(1);
		}
	}
}
