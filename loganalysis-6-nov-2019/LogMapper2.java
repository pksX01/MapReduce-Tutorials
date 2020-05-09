package accenture.loganalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper2 extends Mapper<Object, Text, Text, IntWritable> {
    
    @Override
    protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
    	String lines = value.toString().trim();
		String[] fields = lines.split(",");
		context.write(new Text(fields[1]), new IntWritable(1));
    }
}