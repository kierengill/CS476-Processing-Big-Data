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

public class CleanMapper extends Mapper<Object, Text, Text, Text>{
  
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
        String[] values = line.split(",");
		if (values.length == 31){
			String one = values[0];
			String two = values[4];
			String three = values[5];
			String four = values[8];
			String five = values[12];
			String six = values[13];
			String seven = values[19];
			String eight = values[21];
			String nine = values[22];
            context.write(new Text(one+","+two+","+three+","+four+","+five+","+six+","+seven+","+eight+","+nine), new Text(""));
        }
    }
}