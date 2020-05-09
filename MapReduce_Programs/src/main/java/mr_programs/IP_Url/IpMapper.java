package mr_programs.IP_Url;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IpMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String fields[] = line.split("\t");
		if(fields[3].contains("action") && fields[3].contains("ntr")) {
			context.write(new Text(fields[0]), new IntWritable(1));
		}
	}
}
