package task4;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class DriverClass {
	
	public static void main(String[] args)
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "Airline Negative Sentiments");
		job.setJarByClass(DriverClass.class);
		Path inPath = new Path(otherArgs[0]);
		Path outPath = new Path(otherArgs[1]);
		outPath.getFileSystem(conf).delete(outPath, true);
		
		// Put this file to distributed cache so we can use it to join
		if (otherArgs.length > 2) {
		    job.addCacheFile(new URI(otherArgs[2]));
		}
		
		Configuration validationConf = new Configuration(false);
		ChainMapper.addMapper(job, ANSValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, 
					Text.class, validationConf);
		
		Configuration ansConf = new Configuration(false);
		ChainMapper.addMapper(job, ANSMapper.class, LongWritable.class, Text.class,Text.class, IntWritable.class,
				ansConf);
		
		job.setMapperClass(ChainMapper.class);
		// job.setCombinerClass(ANSReducer.class);
		job.setReducerClass(ANSReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		boolean status = job.waitForCompletion(true);
		
		if (status) {
			System.exit(0);
		} 
		else {
			System.exit(1);
		}
	}
}
	