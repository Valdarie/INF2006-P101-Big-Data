package task2;

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

public class AirlineNegativeSentiments {

    public static void main(String[] args)
            throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // First Job
        Job job1 = Job.getInstance(conf, "Airline Negative Sentiments");
        job1.setJarByClass(AirlineNegativeSentiments.class);
        Path inPath = new Path(otherArgs[0]);
        Path outPath = new Path(otherArgs[1] + "_temp");
        outPath.getFileSystem(conf).delete(outPath, true);

        // Put this file to distributed cache so we can use it to join
        job1.addCacheFile(new URI(otherArgs[2]));

        Configuration validationConf = new Configuration(false);
        ChainMapper.addMapper(job1, ANSValidationMapper.class, LongWritable.class, Text.class, LongWritable.class,
                Text.class, validationConf);

        Configuration ansConf = new Configuration(false);
        ChainMapper.addMapper(job1, ANSMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class,
                ansConf);

        job1.setMapperClass(ChainMapper.class);
        job1.setReducerClass(ANSReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, inPath);
        FileOutputFormat.setOutputPath(job1, outPath);
        boolean status1 = job1.waitForCompletion(true);

        // Run the second job if the first job completes successfully
        if (status1) {
            // Second Job (Top Negative Reasons)
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Top Negative Reasons");
            job2.setJarByClass(TopReasonsDriver.class);

            job2.setMapperClass(TopReasonsMapper.class);
            job2.setReducerClass(TopReasonsReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, outPath);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

            boolean status2 = job2.waitForCompletion(true);

            System.exit(status2 ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
