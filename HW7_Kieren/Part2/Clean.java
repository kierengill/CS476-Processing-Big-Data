import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Clean {
  public static void main(String[] args) throws Exception {
    // Configuration conf = new Configuration();
    // Job job = Job.getInstance(conf, "count lines");
    // job.setNumReduceTasks(1);
    // job.setJarByClass(CountLines.class);
    // job.setMapperClass(CountRecsMapper.class);
    // job.setReducerClass(CountRecsReducer.class);
    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(IntWritable.class);
    // FileInputFormat.addInputPath(job, new Path(args[0]));
    // FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
    Job job = new Job();
		job.setJarByClass(Clean.class);
		job.setJobName("Count Lines");
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CleanMapper.class);
		job.setReducerClass(CleanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}