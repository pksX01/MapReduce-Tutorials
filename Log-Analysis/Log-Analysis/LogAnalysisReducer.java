package com.accenture.hadoop.loganalysis;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogAnalysisReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
		Set <String> distinctIps = new TreeSet<String>();
		// Iterating the list of values and add to set to remove duplicates
		for (Text ip : value) {
			distinctIps.add(ip.toString());
		}
		context.write(key, new Text(distinctIps.toString()));
	}
}