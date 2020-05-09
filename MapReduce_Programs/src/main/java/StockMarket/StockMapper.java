package StockMarket;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StockMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String fields[] = value.toString().split(",");
		String stock = fields[0] + "\t" + fields[1];
		context.write(NullWritable.get(), new Text(stock));
	}
}
