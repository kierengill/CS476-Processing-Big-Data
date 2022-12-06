import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyticsMapper extends Mapper<Object, Text, Text, Text>{
  
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
        String[] values = line.split(",");
        try{
		    String severity_code = values[2];
            String incident_response = values[4];
            int test = Integer.parseInt(severity_code);
            int test2 = Integer.parseInt(incident_response);
            String borough = values[6];
            String count = "1";
            context.write(new Text(borough), new Text(severity_code + "," + incident_response + "," + count));
        }
        catch(Exception ex){
        }
    }
}