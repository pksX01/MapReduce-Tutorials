package com.accenture.hadoop.loganalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogAnalysisMapper extends Mapper<Object, Text, Text, Text> {

	protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		// split the input string by 'space' delimiter
		String lines = value.toString().trim();
		String[] fields = lines.split(",");
		context.write(new Text(fields[1]), new Text(fields[0]));
	}
}
