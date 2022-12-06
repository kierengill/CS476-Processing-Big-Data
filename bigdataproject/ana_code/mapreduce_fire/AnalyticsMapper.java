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

public class AnalyticsMapper
    extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    public static int number = 0;

    public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
        String input = new String(value.toString());
        String[] sentence = input.split(",");
        try {
            String incidentResponseTime = sentence[13];
            String engines = sentence[15];
            int testing = Integer.parseInt(incidentResponseTime);
            int testing2 = Integer.parseInt(engines);
            String count = "1";
            word.set(sentence[3]);
            context.write(word, new Text(incidentResponseTime + "," + engines + "," + count));
        }catch (Exception e) {
            }
        }
        
}