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

public class AnalyticsReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        
        double count = 0;
        double severitySum = 0;
        double responseSum = 0;
        double sum = 0;

        for (Text val : values){
            String line = val.toString();
            String[] word = line.split(",");
            double severity_code = Double.parseDouble(word[0]);
            double incident_response = Double.parseDouble(word[1]);
            count = Double.parseDouble(word[2]);
            severitySum += severity_code;
            responseSum += incident_response;
            sum+=count;
        }
		context.write(key, new Text(String.valueOf(severitySum) + "," + String.valueOf(responseSum) + "," + String.valueOf(sum)));
	}
}