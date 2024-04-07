/**
Done By Seri Hanzalah
 */

package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CountryDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: CountryDriver <in> <out> <country-codes>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Airline Complaints by Country");
        job.setJarByClass(CountryDriver.class);

        // Add the country codes file to the distributed cache
        job.addCacheFile(new URI(otherArgs[2]));

        // Chain Mapper setup
        Configuration validationMapConf = new Configuration(false);
        ChainMapper.addMapper(job, CountryMapper.class, Object.class, Text.class, Text.class, IntWritable.class, validationMapConf);

        job.setReducerClass(CountryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
