package devices;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForAvg extends Mapper<LongWritable, Text,  NullWritable,IntWritable>{
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		String fields[] = values.toString().split("\t");
		context.write(NullWritable.get(), new IntWritable(Integer.parseInt(fields[2])));
	}
}
