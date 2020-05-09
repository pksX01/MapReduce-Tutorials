package devices;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, NullWritable, Text>{ 
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		String line = values.toString();
		String fields[] = line.split("\t");
		if(fields[1].equalsIgnoreCase("mobile"))
			context.write(NullWritable.get(), values);
	}
}
