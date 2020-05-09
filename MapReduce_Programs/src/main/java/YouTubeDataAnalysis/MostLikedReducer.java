package YouTubeDataAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostLikedReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		long max = -1;
		String videoId="",title="",channel="",publish_time="",views="";
		long no_of_likes=0;
		for(Text val : value) {
			String fields[] = val.toString().split("\t");
			//no_of_likes = Long.parseLong(fields[5]);
			no_of_likes = Long.parseLong(fields[2]);
			if(no_of_likes > max) {
				max = no_of_likes;
				/*videoId = fields[0];
				title = fields[1];
				channel = fields[2];
				publish_time = fields[3];
				views = fields[4];*/
				title = fields[0];
				views = fields[1];
			}
		}
		
		String reducerOp = /*videoId + "\t" + */title + "\t" /* + channel + "\t" + publish_time + "\t" */ + views + "\t" + Long.valueOf(no_of_likes);
		context.write(key, new Text(reducerOp));
	}
}
