package task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DriverClass {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: chainmapper <in> <out>");
			System.exit(2);
		}

		// Configure the job
		Job job = Job.getInstance(conf, "Mean Median Calc based on Channel");
		job.setJarByClass(DriverClass.class);

		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);

		// Clear the output directory
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
			System.out.println("Existing output path deleted.");
		}

		// Configure the ChainMapper to use ANSValidationMapper first
		Configuration validationMapConf = new Configuration(false);
		ChainMapper.addMapper(job, T4ValidationMapper.class, LongWritable.class, Text.class, LongWritable.class,
				Text.class, validationMapConf);

		// Configure the ChainMapper to use ANSMapper second
		Configuration ansMapConf = new Configuration(false);
		ChainMapper.addMapper(job, T4Mapper.class, LongWritable.class, Text.class, Text.class, FloatWritable.class,
				ansMapConf);

		// Set ANSReducer as the reducer class
		job.setReducerClass(T4Reducer.class);

		// Set the input and output paths
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Since the reducer's output is Text for both key and value, set them
		// accordingly
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Execute the job and wait for completion
		boolean jobCompletedSuccessfully = job.waitForCompletion(true);

		if (jobCompletedSuccessfully) {
			System.out.println("Job executed successfully.");
			System.exit(0);
		} else {
			System.err.println("Job execution failed.");
			System.exit(1);
		}
	}
}
