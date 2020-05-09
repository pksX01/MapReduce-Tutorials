package YouTubeDataAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostLikedMapper extends Mapper<Object, Text, IntWritable, Text>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String fields[] = new String[8];
		fields = value.toString().trim().split(",");
		int category = Integer.parseInt(fields[4]);
		String str = /*fields[0] + "\t" + */fields[2] /*+ "\t" + fields[3] + "\t" + fields[5] + "\t" */+ fields[7] + "\t" + fields[8];
		context.write(new IntWritable(category), new Text(str));
	}
}
