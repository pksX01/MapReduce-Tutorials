package devices;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.bag.TreeBag;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper2 extends Mapper<LongWritable,Text,NullWritable,Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String fields[] = value.toString().split("\t");
		
		context.write(NullWritable.get(), new Text(fields[1]));
	}
}
