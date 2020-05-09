package devices;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<NullWritable,Text,NullWritable,Text>{
	public void reduce(NullWritable key, Text values, Context context) throws IOException, InterruptedException{
		context.write(key, values);
	}
}
