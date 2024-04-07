/**
 * 2202217 Teo Xuanting
 */

package task1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.OutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataCleaning {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Data Cleaning");
		job.setJarByClass(DataCleaning.class);
		job.setMapperClass(CleanDataMapper.class);
		job.setReducerClass(CleanDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Wait for the job to complete
		if (job.waitForCompletion(true)) {
			// Rename the output file
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] files = fs.listStatus(outputPath);
			for (FileStatus file : files) {
				if (file.getPath().getName().startsWith("part-r-")) {
					Path oldFilePath = file.getPath();
					Path newFilePath = new Path(outputPath, "cleaned_data.csv"); // New file path inside the "task1" folder
					fs.rename(oldFilePath, newFilePath);
					break;
				}
			}
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}