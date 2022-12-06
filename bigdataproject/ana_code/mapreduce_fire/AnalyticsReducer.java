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

public class AnalyticsReducer
    extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double responseTime = 0;
      double engines = 0;
      double count = 0;
      double responseSum = 0;
      double enginesSum = 0;
      double sum = 0;
      for (Text val : values) {
        String input = new String(val.toString());
        String[] sentence = input.split(",");
        responseTime = Double.parseDouble(sentence[0]);
        engines = Double.parseDouble(sentence[1]);
        count = Double.parseDouble(sentence[2]);
        responseSum += responseTime;
        enginesSum += engines;
        sum += count;
      }
      
      context.write(key, new Text(String.valueOf(responseSum) + "," + String.valueOf(enginesSum) + "," + String.valueOf(sum)));
    }
  }