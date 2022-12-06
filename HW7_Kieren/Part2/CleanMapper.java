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

public class CleanMapper
    extends Mapper<Object, Text, Text, Text> {
        
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String input = new String(value.toString());
      String[] sentence = input.split(",");
      if (sentence.length == 29){
        String id = sentence[0];
        String date = sentence[1];
        String alarmNumber = sentence[3];
        String incidentBorough = sentence[5];
        String zip = sentence[6];
        String policePrecinct = sentence[7];
        String alarmDescription = sentence[12];
        String highestAlarm = sentence[14];
        String classification = sentence[15];
        String dispatchResponse = sentence[17];
        String assignment = sentence[18];
        String activation = sentence[19];
        String onScene = sentence[20];
        String incidentResponseTime = sentence[24];
        String incidentTravelTime = sentence[25];
        String engines = sentence[26];
        String otherUnits = sentence[28];
        context.write(new Text(id + "," + date + "," + alarmNumber + "," + incidentBorough + "," + zip + "," + policePrecinct + "," + alarmDescription + "," + highestAlarm + "," + classification + "," + dispatchResponse + "," + assignment + "," + activation + "," + onScene + "," + incidentResponseTime + "," + incidentTravelTime + "," + engines + "," + otherUnits), new Text(""));
    }
    }
}
