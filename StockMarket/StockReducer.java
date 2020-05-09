package StockMarket;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StockReducer extends Reducer< NullWritable, Text,NullWritable, Text> {
	private float highest = Float.MIN_VALUE;
	private String highDate;

	public void reduce(NullWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		
		for (Text val: value) {
			String data[] = val.toString().split("\t");
			String date = data[0];
			float price = Float.parseFloat(data[1]);
			if(price>highest) {
				highest=price;
				highDate=date;
			}
		}
		Text reducerOp = new Text(highDate + "\t" + Float.valueOf(highest));
		
		context.write(key, reducerOp);
	}
}
