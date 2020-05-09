package devices;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<NullWritable,Text,NullWritable,Text>{
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Set<String> distinctVal = new TreeSet<String>();
		for(Text val: values) {
			distinctVal.add(val.toString());
		}
		context.write(key, new Text(distinctVal.toString()));
	}
}
